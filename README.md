# Jobpool

Copyright 2013 Ardan Studios. All rights reserved.  
Use of this source code is governed by a BSD-style license that can be found in the LICENSE handle.

Package jobpool implements a pool of go routines that are dedicated to processing jobs posted into the pool. The jobpool maintains two queues, a normal processing queue and a priority queue. Jobs placed in the priority queue will be processed ahead of pending jobs in the normal queue.

Ardan Studios  
12973 SW 112 ST, Suite 153  
Miami, FL 33186  
bill@ardanstudios.com

[Click To View Documentation](http://godoc.org/github.com/goinggo/jobpool)