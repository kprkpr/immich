import BackgroundTasks
import Photos
import SQLiteData
import StructuredFieldValues

class UploadApiImpl: NSObject, UploadApi, URLSessionDataDelegate, URLSessionTaskDelegate {
  let maxActiveDownloads = 5
  let maxActiveUploads = 3
  let maxPendingUploads = 5
  let maxAttempts = 3

  private static let resourceManager = PHAssetResourceManager.default()
  private static let assetNotFound = Result<Int64, any Error>.failure(
    PigeonError(code: "ASSET_NOT_FOUND", message: nil, details: nil)
  )
  private static var activeUploads: [Int: Int64] = [:]  // sessionTaskId -> taskId
  private static var downloadTasks: [Int64: Task<Void, Never>] = [:]
  private static var uploadTasks: [Int64: Task<Void, Never>] = [:]
  private static var queueProcessingTask: Task<Void, Never>?

  private let pool: DatabasePool
  private let uploadSession: URLSession

  override init() {
    let config = URLSessionConfiguration.background(withIdentifier: "app.mertalev.immich.upload")
    config.sessionSendsLaunchEvents = false
    self.pool = try! createDatabase() // TODO: error handling
    self.uploadSession = URLSession(configuration: config)
    super.init()
    Task { await self.recoverPendingTasks() }
  }

  func createUploadTask(
    accessToken: String,
    checksum: String,
    cloudId: String?,
    deviceId: String,
    group: String,
    livePhotoVideoId: String?,
    localId: String,
    url: String,
    completion: @escaping (Result<Int64, any Error>) -> Void
  ) {
    Task {
      guard let asset = PHAsset.fetchAssets(withLocalIdentifiers: [localId], options: nil).firstObject
      else {
        return completion(Self.assetNotFound)
      }

      let draft = UploadTask.Draft(
        accessToken: accessToken,
        attempts: 0,
        checksum: checksum,
        cloudId: cloudId,
        createdAt: Date(),
        deviceAssetId: asset.localIdentifier,
        deviceId: deviceId,
        fileCreatedAt: Int64(asset.creationDate!.timeIntervalSince1970),
        fileModifiedAt: Int64(asset.modificationDate!.timeIntervalSince1970),
        fileName: asset.title,
        filePath: nil,
        group: group,
        isFavorite: asset.isFavorite,
        livePhotoVideoId: livePhotoVideoId,
        localId: localId,
        remoteId: nil,
        sessionTaskId: nil,
        status: .pendingDownload,
        url: url,
      )

      do {
        let task = try await pool.write { conn in
          try UploadTask.insert { draft }.returning(\.self).fetchOne(conn)!
        }
        startQueueProcessing()
        completion(.success(task.id))
      } catch {
        completion(.failure(error))
      }
    }
  }

  func cancelTask(_ taskId: Int64) async {
    if let downloadTask = Self.downloadTasks.removeValue(forKey: taskId) {
      downloadTask.cancel()
    }

    if let uploadTask = Self.uploadTasks.removeValue(forKey: taskId) {
      uploadTask.cancel()
    }

    if let sessionTaskId = Self.activeUploads.first(where: { $0.value == taskId })?.key {
      let tasks = await uploadSession.allTasks
      tasks.first(where: { $0.taskIdentifier == sessionTaskId })?.cancel()
      Self.activeUploads.removeValue(forKey: sessionTaskId)
    }

    try? await pool.write { conn in
      try UploadTask.delete().where { $0.id.eq(taskId) }.execute(conn)
    }
  }

  private func startQueueProcessing() {
    guard Self.queueProcessingTask == nil else { return }

    Self.queueProcessingTask = Task {
      await processQueue()
      Self.queueProcessingTask = nil
    }
  }

  private func processQueue() async {
    await withTaskGroup(of: Void.self) { group in
      await startDownloads(in: &group)
      await startUploads(in: &group)
    }
  }

  private func startDownloads(in group: inout TaskGroup<Void>) async {
    guard
      let tasks = try? await pool.read({ conn in
        return try UploadTask.where {
          $0.status.in([TaskStatus.pendingDownload, TaskStatus.downloadFailed]) && $0.attempts < maxAttempts
        }
        .order { $0.createdAt }
        .limit { maxActiveDownloads - $0.count(filter: $0.status.eq(TaskStatus.downloading)) }
        .fetchAll(conn)
      })
    else { return }

    for task in tasks {
      let downloadTask = Task { await downloadAndQueue(task) }
      Self.downloadTasks[task.id] = downloadTask
      group.addTask { await downloadTask.value }
    }
  }

  private func downloadAndQueue(_ task: UploadTask) async {
    defer {
      Self.downloadTasks.removeValue(forKey: task.id)
      startQueueProcessing()
    }

    guard let asset = PHAsset.fetchAssets(withLocalIdentifiers: [task.localId], options: nil).firstObject,
      let resource = asset.getResource()
    else {
      return await handleDownloadFailure(taskId: task.id, attempt: task.attempts)
    }

    let filePath: URL
    do {
      filePath = try getPath(resource)
      try await pool.write { conn in
        try UploadTask.update { row in
          row.status = .downloading
          row.filePath = filePath.path
        }
        .where { $0.id.eq(task.id) }
        .execute(conn)
      }
    } catch {
      return await handleDownloadFailure(taskId: task.id, attempt: task.attempts)
    }

    do {
      for try await _ in downloadAsset(resource: resource, to: filePath) {} // TODO: progress events
      try await pool.write { conn in
        try UploadTask.update {
          $0.attempts = 0
          $0.status = .pendingUpload
        }.where { $0.id.eq(task.id) }.execute(conn)
      }
    } catch {
      print("Download failed for task \(task.id) (attempt \(task.attempts)): \(error.localizedDescription)")
      await handleDownloadFailure(taskId: task.id, attempt: task.attempts)
    }
  }

  func downloadAsset(resource: PHAssetResource, to filePath: URL) -> AsyncThrowingStream<Double, Error> {
    AsyncThrowingStream { continuation in
      let options = PHAssetResourceRequestOptions()
      options.isNetworkAccessAllowed = true
      options.progressHandler = { progress in
        continuation.yield(progress)
      }

      Self.resourceManager.writeData(for: resource, toFile: filePath, options: options) { error in
        if let error = error {
          continuation.finish(throwing: error)
        } else {
          continuation.yield(1.0)
          continuation.finish()
        }
      }

      continuation.onTermination = { termination in
        if case .cancelled = termination {
          try? FileManager.default.removeItem(at: filePath)
        }
      }
    }
  }

  private func handleDownloadFailure(taskId: Int64, attempt: Int) async {
    let newAttempts = attempt + 1
    do {
      try await pool.write { conn in
        try UploadTask.update { row in
          row.status = .downloadFailed
          row.attempts = newAttempts
        }
        .where { $0.id.eq(taskId) }
        .execute(conn)
      }
    } catch {
      print("Failed to update download failure status for task \(taskId): \(error)")
    }
  }

  private func startUploads(in group: inout TaskGroup<Void>) async {
    guard
      let tasks = try? await pool.read({ conn in
        try UploadTask.where {
          $0.status.in([TaskStatus.pendingUpload, TaskStatus.uploadFailed]) && $0.attempts < maxAttempts
        }
        .order { $0.createdAt }
        .limit { maxActiveUploads - $0.count(filter: $0.status.in([TaskStatus.uploadQueued, TaskStatus.uploading])) }
        .fetchAll(conn)
      })
    else { return }

    for task in tasks {
      let uploadTask = Task { await startUpload(task) }
      Self.uploadTasks[task.id] = uploadTask
      group.addTask { await uploadTask.value }
    }
  }

  private func startUpload(_ task: UploadTask) async {
    defer {
      Self.uploadTasks.removeValue(forKey: task.id)
    }

    guard let filePath = task.filePath else {
      return print("Upload failed for \(task.id): no file path")
    }

    let assetData: String
    do {
      let encoder = StructuredFieldValueEncoder()
      assetData = String(bytes: try encoder.encode(AssetData(task: task)), encoding: .utf8)!
    } catch {
      return print("Upload failed for \(task.id): \(error.localizedDescription)")
    }

    var request = URLRequest(url: URL(string: task.url)!)
    request.httpMethod = "POST"
    request.setValue(task.accessToken, forHTTPHeaderField: UploadHeaders.userToken.rawValue)
    request.setValue(assetData, forHTTPHeaderField: UploadHeaders.assetData.rawValue)
    request.setValue("sha=:\(task.checksum):", forHTTPHeaderField: UploadHeaders.reprDigest.rawValue)

    let sessionTask = uploadSession.uploadTask(with: request, fromFile: URL(fileURLWithPath: filePath))

    do {
      try await pool.write { conn in
        try UploadTask.update { row in
          row.status = .uploadQueued
          row.sessionTaskId = sessionTask.taskIdentifier
        }
        .where { $0.id.eq(task.id) }
        .execute(conn)
      }

      Self.activeUploads[sessionTask.taskIdentifier] = task.id
      try? FileManager.default.removeItem(at: URL(fileURLWithPath: filePath))  // upload task already copied the file
      sessionTask.resume()
    } catch {
      print("Upload failed for \(task.id): could not start upload: \(error.localizedDescription)")
    }
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
    guard let taskId = Self.activeUploads.removeValue(forKey: task.taskIdentifier) else { return }
    Task {
      switch error {
      case let urlError as URLError where urlError.code == .cancelled:
        try? await pool.write { conn in
          try UploadTask.delete().where { $0.id.eq(taskId) }.execute(conn)
        }
      case let urlError as URLError:
        await handleUploadFailure(taskId: taskId, error: urlError, sessionTask: task)
      case let error?:
        try? await updateStatus(.uploadFailed, taskId: taskId)
      case nil:
        await handleUploadSuccess(taskId: taskId)
      }

      startQueueProcessing()
    }
  }

  private func handleUploadSuccess(taskId: Int64) async {
    do {
      try await updateStatus(.uploadComplete, taskId: taskId)
    } catch {
      print("Failed to update upload success status for task \(taskId): \(error.localizedDescription)")
    }
  }

  private func handleUploadFailure(taskId: Int64, error: URLError, sessionTask: URLSessionTask) async {
    try? await updateStatus(.uploadFailed, taskId: taskId)
    if #available(iOS 17, *), let resumeData = error.uploadTaskResumeData {
      let resumeTask = uploadSession.uploadTask(withResumeData: resumeData)
      Self.activeUploads[resumeTask.taskIdentifier] = taskId
      resumeTask.resume()
    }
  }

  private func recoverPendingTasks() async {
    try? await pool.write { conn in
      let tasks = try UploadTask.update { $0.status = TaskStatus.pendingDownload }
        .where { $0.status.in([TaskStatus.downloadQueued, TaskStatus.downloading]) }
        .returning(\.self)
        .fetchAll(conn)

      for task in tasks {
        if let filePath = task.filePath {
          try? FileManager.default.removeItem(at: URL(fileURLWithPath: filePath))
        }
      }
    }

    startQueueProcessing()
  }

  private func updateStatus(_ status: TaskStatus, taskId: Int64) async throws {
    try await pool.write { conn in
      try UploadTask.update { $0.status = status }.where { $0.id.eq(taskId) }.execute(conn)
    }
  }

  private func getQueueStats() async throws -> QueueStats {
    return try await pool.read({ conn in
      return try UploadTask.select { row in
        return QueueStats.Columns(
          activeDownloads: row.count(filter: row.status.eq(TaskStatus.downloading)),
          activeUploads: row.count(filter: row.status.eq(TaskStatus.uploading)),
          pendingUploads: row.count(filter: row.status.eq(TaskStatus.uploadQueued)),
        )
      }.fetchOne(conn) ?? QueueStats(activeDownloads: 0, activeUploads: 0, pendingUploads: 0)
    })
  }

  private func getPath(_ resource: PHAssetResource) throws -> URL {
    let fileDir = FileManager.default.temporaryDirectory.appendingPathComponent("originals", isDirectory: true)
    try FileManager.default.createDirectory(at: fileDir, withIntermediateDirectories: true, attributes: nil)
    return fileDir.appendingPathComponent("\(resource.assetLocalIdentifier)_\(resource.type.rawValue)")
  }
}
