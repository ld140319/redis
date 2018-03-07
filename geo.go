package redis

func (cp *ConnPool) GEOADD() (err error) {
	if cp.lessThan("3.2.0") {
		return ErrNotSupport
	}
	// @todo
	return
}

func (cp *ConnPool) GEODIST() (err error) {
	if cp.lessThan("3.2.0") {
		return ErrNotSupport
	}
	// @todo
	return
}

func (cp *ConnPool) GEOHASH() (err error) {
	if cp.lessThan("3.2.0") {
		return ErrNotSupport
	}
	// @todo
	return
}

func (cp *ConnPool) GEOPOS() (err error) {
	if cp.lessThan("3.2.0") {
		return ErrNotSupport
	}
	// @todo
	return
}

func (cp *ConnPool) GEORADIUS() (err error) {
	if cp.lessThan("3.2.0") {
		return ErrNotSupport
	}
	// @todo
	return
}

func (cp *ConnPool) GEORADIUSBYMEMBER() (err error) {
	if cp.lessThan("3.2.0") {
		return ErrNotSupport
	}
	// @todo
	return
}
