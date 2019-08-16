package nos

type Multi struct {
	Key      string
	UploadId int64
}

func newMulti(upload *Upload) (*Multi) {
	return &Multi{
		Key: upload.Key,
		UploadId: upload.UploadId,
	}
}

func initMulti(result *InitMultiUploadResult) (*Multi) {
	return &Multi{
		Key: result.Object,
		UploadId: result.UploadId,
	}
}