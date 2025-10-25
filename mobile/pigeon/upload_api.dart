import 'package:pigeon/pigeon.dart';

@ConfigurePigeon(
  PigeonOptions(
    dartOut: 'lib/platform/upload_api.g.dart',
    swiftOut: 'ios/Runner/Upload/UploadTask.g.swift',
    swiftOptions: SwiftOptions(includeErrorClass: false),
    kotlinOut: 'android/app/src/main/kotlin/app/alextran/immich/upload/UploadTask.g.kt',
    kotlinOptions: KotlinOptions(package: 'app.alextran.immich.upload'),
    dartOptions: DartOptions(),
    dartPackageName: 'immich_mobile',
  ),
)
@HostApi()
abstract class UploadApi {
  @async
  int createUploadTask({
    required String accessToken,
    required String checksum,
    String? cloudId,
    required String deviceId,
    required String group,
    String? livePhotoVideoId,
    required String localId,
    required String url,
  });
}
