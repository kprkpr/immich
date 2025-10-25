import SQLiteData

enum UploadStatus: Int, QueryBindable {
  case pending, queued, inProgress, complete, paused, failed
}

enum TaskStatus: Int, QueryBindable {
  case pendingDownload, downloadQueued, downloadFailed, downloading, pendingUpload, uploadQueued, uploadFailed, uploading, uploadComplete
}

enum DownloadStatus: Int, QueryBindable {
  case notStarted, downloading, downloaded, failed
}

enum UploadHeaders: String {
  case reprDigest = "Repr-Digest"
  case userToken = "X-Immich-User-Token"
  case assetData = "X-Immich-Asset-Data"
}
