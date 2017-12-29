# rpcx client for python
## what is rpcx?
please to see [rpcx.site](http://rpcx.site/) or [github](https://github.com/smallnest/rpcx)!

## example
***service***
```golang
package main

import (
	"context"
	"flag"

	"github.com/smallnest/rpcx/server"
)

var (
	addr = flag.String("addr", "localhost:8972", "127.0.0.1")
)

type Arith struct{}

type Args struct {
	A int
	B int
}

type Reply struct {
	C int
}


// the second parameter is not a pointer
func (t *Arith) Mul(ctx context.Context, args *Args, reply *Reply) error {
	reply.C = args.A * args.B
	return nil
}

func main() {
	flag.Parse()

	s := server.NewServer()
	s.Register(new(Arith), "")
	s.Serve("tcp", *addr)
}
```

***client***
```python
from rpcx import Client
client = Client('localhost', 8972)
response = client.call('Arith', 'Mul', dict(A=2, B=3))
print(response.success)
if response.success:
    print(response.payload)
else:
    print(response.error)
```

***contact***
- mail: hyhkjiy@163.com
- welcome submit issues and send PRs！