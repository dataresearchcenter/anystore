from s3fs.core import S3FileSystem as BaseS3FileSystem


class S3FileSystem(BaseS3FileSystem):
    def info(self, path, **kwargs):
        info = super().info(path, **kwargs)
        info["created"] = info["LastModified"]
        return info
