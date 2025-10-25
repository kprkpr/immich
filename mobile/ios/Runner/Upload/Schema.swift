import GRDB
import SQLiteData
import StructuredFieldValues

@Table
struct UploadTask {
  @Column(primaryKey: true)
  let id: Int64
  let accessToken: String
  let attempts: Int
  let checksum: String
  let cloudId: String?
  let createdAt: Date
  let deviceAssetId: String
  let deviceId: String
  let fileCreatedAt: Int64
  let fileModifiedAt: Int64
  let fileName: String
  let filePath: String?
  let group: String
  let isFavorite: Bool
  let livePhotoVideoId: String?
  let localId: String
  let remoteId: String?
  let sessionTaskId: Int?
  let status: TaskStatus
  let url: String
}

@Selection
struct QueueStats {
  let activeDownloads: Int
  let activeUploads: Int
  let pendingUploads: Int
}

struct AssetData: StructuredFieldValue {
  static let structuredFieldType: StructuredFieldType = .dictionary

  init(task: UploadTask) {
    self.deviceAssetId = task.deviceAssetId
    self.deviceId = task.deviceId
    self.fileCreatedAt = task.fileCreatedAt
    self.fileModifiedAt = task.fileModifiedAt
    self.fileName = task.fileName
    self.iCloudId = task.cloudId
    self.isFavorite = task.isFavorite
    self.livePhotoVideoId = task.livePhotoVideoId
  }

  let deviceAssetId: String
  let deviceId: String
  let fileCreatedAt: Int64
  let fileModifiedAt: Int64
  let fileName: String
  let iCloudId: String?
  let isFavorite: Bool?
  let livePhotoVideoId: String?
}

func createDatabase() throws -> DatabasePool {
  let fileManager = FileManager.default
  let supportDir = try fileManager.url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)
  let databaseDir = supportDir.appendingPathComponent("UploadDatabase", isDirectory: true)
  try fileManager.createDirectory(at: databaseDir, withIntermediateDirectories: false)
  let databaseURL = databaseDir.appendingPathComponent("db.sqlite")
  let db = try DatabasePool(path: databaseURL.path)
  try runMigrations(db)
  return db
}

func runMigrations(_ db: DatabaseWriter) throws {
  var migrator = DatabaseMigrator()
  #if DEBUG
    migrator.eraseDatabaseOnSchemaChange = true
  #endif

  migrator.registerMigration("Create upload_tasks table") { db in
    let options: TableOptions = if #available(iOS 15.4, *) { [.strict] } else { [] }
    try db.create(table: "upload_tasks", options: options) { t in
      t.primaryKey("id", .integer)
      t.column("accessToken", .text).notNull()
      t.column("attempts", .integer).notNull()
      t.column("checksum", .blob).notNull()
      t.column("cloudId", .text)
      t.column("createdAt", .datetime).notNull()
      t.column("deviceAssetId", .text).notNull()
      t.column("deviceId", .text).notNull()
      t.column("fileCreatedAt", .datetime).notNull()
      t.column("fileModifiedAt", .datetime).notNull()
      t.column("fileName", .text).notNull()
      t.column("filePath", .text)
      t.column("group", .text).notNull()
      t.column("isFavorite", .boolean)
      t.column("livePhotoVideoId", .text)
      t.column("localId", .text)
      t.column("remoteId", .integer)
      t.column("sessionTaskId", .integer)
      t.column("status", .integer).notNull()
      t.column("url", .text).notNull()
    }
  }

  try migrator.migrate(db)
}
