package nos

import (
	"encoding/xml"
)

// CompleteMultiUpload
type UploadPart struct {
	XMLName    xml.Name `xml:"Part"`
	PartNumber int32    `xml:"PartNumber"`
	Etag       string   `xml:"ETag"`
	Size       int64    `xml:"Size,omitempty"`
}

type UploadParts struct {
	XMLName xml.Name     `xml:"CompleteMultipartUpload"`
	Parts   []UploadPart `xml:"Part"`
}

func (uploadParts *UploadParts) Append(part UploadPart) {
	uploadParts.Parts = append(uploadParts.Parts, part)
}

// DeleteMultiObjects
type DeleteObject struct {
	XMLName xml.Name `xml:"Object"`
	Key     string   `xml:"Key"`
}

type DeleteMultiObjects struct {
	XMLName xml.Name       `xml:"Delete"`
	Quiet   bool           `xml:"Quiet"`
	Objects []DeleteObject `xml:"Object"`
}

func (deleteMulti *DeleteMultiObjects) Append(object DeleteObject) {
	deleteMulti.Objects = append(deleteMulti.Objects, object)
}
