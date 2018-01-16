package filter

import "bytes"

// influxUnescape returns a new slice containing the unescaped version
// of in.
//
// This is the same as Unescape() from
// github.com/influxdata/influxdb/pkg/escape.
// It's copied here because it's not worth vendoring all of influxdb
// just for this.
func influxUnescape(in []byte) []byte {
	if bytes.IndexByte(in, '\\') == -1 {
		return in
	}

	i := 0
	inLen := len(in)

	// The output will be no more than inLen. Preallocating the
	// capacity here is faster and uses less memory than letting
	// append allocate.
	out := make([]byte, 0, inLen)

	for {
		if i >= inLen {
			break
		}
		if in[i] == '\\' && i+1 < inLen {
			switch in[i+1] {
			case ',':
				out = append(out, ',')
				i += 2
				continue
			case '"':
				out = append(out, '"')
				i += 2
				continue
			case ' ':
				out = append(out, ' ')
				i += 2
				continue
			case '=':
				out = append(out, '=')
				i += 2
				continue
			}
		}
		out = append(out, in[i])
		i += 1
	}
	return out
}
