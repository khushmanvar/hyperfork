// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import "errors"

// ErrCorruption is returned when the database is corrupt.
var ErrCorruption = errors.New("pebble: corruption") 