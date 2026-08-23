package pdf

import (
	"bytes"
	"fmt"
	"strings"
	"unicode/utf16"
)

func renderDocument(lines []string, watermark string) []byte {
	if len(lines) == 0 {
		return nil
	}
	var stream strings.Builder
	stream.WriteString("BT\n/F1 12 Tf\n50 790 Td\n16 TL\n")
	if watermark != "" {
		stream.WriteString("0.85 g\n")
		stream.WriteString(pdfHex(watermark))
		stream.WriteString(" Tj\n0 g\n0 -32 Td\n")
	}
	for index, line := range lines {
		if index > 0 {
			stream.WriteString("T*\n")
		}
		stream.WriteString(pdfHex(line))
		stream.WriteString(" Tj\n")
	}
	stream.WriteString("ET\n")
	objects := []string{"<< /Type /Catalog /Pages 2 0 R >>", "<< /Type /Pages /Kids [3 0 R] /Count 1 >>", "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>", fmt.Sprintf("<< /Length %d >>\nstream\n%sendstream", stream.Len(), stream.String()), "<< /Type /Font /Subtype /Type0 /BaseFont /STSong-Light /Encoding /UniGB-UCS2-H /DescendantFonts [6 0 R] >>", "<< /Type /Font /Subtype /CIDFontType0 /BaseFont /STSong-Light /CIDSystemInfo << /Registry (Adobe) /Ordering (GB1) /Supplement 4 >> >>"}
	var output bytes.Buffer
	output.WriteString("%PDF-1.7\n%\xE2\xE3\xCF\xD3\n")
	offsets := make([]int, len(objects)+1)
	for index, object := range objects {
		offsets[index+1] = output.Len()
		fmt.Fprintf(&output, "%d 0 obj\n%s\nendobj\n", index+1, object)
	}
	xref := output.Len()
	fmt.Fprintf(&output, "xref\n0 %d\n0000000000 65535 f \n", len(objects)+1)
	for index := 1; index < len(offsets); index++ {
		fmt.Fprintf(&output, "%010d 00000 n \n", offsets[index])
	}
	fmt.Fprintf(&output, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(objects)+1, xref)
	return output.Bytes()
}
func pdfHex(value string) string {
	units := utf16.Encode([]rune(value))
	var builder strings.Builder
	builder.WriteString("<FEFF")
	for _, unit := range units {
		fmt.Fprintf(&builder, "%04X", unit)
	}
	builder.WriteByte('>')
	return builder.String()
}
