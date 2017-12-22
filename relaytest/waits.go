// Copyright 2017 Jump Trading
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package relaytest

import "time"

// LongWait should be used in tests when waiting for events that are
// expected to happen. It is quite long to account for slow/busy test
// hosts. Typically only a fraction of this time is actually used,
// unless something is broken.
const LongWait = 10 * time.Second

// ShortWait should be used in tests when checking for events that are
// expected *not* to happen. A test will block for this long, unless
// something is broken.
const ShortWait = 50 * time.Millisecond
