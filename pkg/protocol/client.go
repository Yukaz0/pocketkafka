package protocol

func readInt64Array(r *Reader) ([]int64, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	out := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		v, err := r.ReadInt64()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
