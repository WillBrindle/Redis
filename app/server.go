package main

import (
  "fmt";
  "os";
  "net";
  "io"
)

func handleConnection(conn net.Conn) {
  fmt.Println("Connection accepted")

  tmp := make([]byte, 256)
  for {
    _, err := conn.Read(tmp)
    if err != nil {
      if err != io.EOF {
        fmt.Println("read error:", err)
      }
      break
     }
    
     conn.Write([]byte("+PONG\r\n"))
  }

  conn.Close()
}

func main() {
  fmt.Println("Launching server!")

  l, err := net.Listen("tcp", "0.0.0.0:6379")
  if err != nil {
    fmt.Println("Failed to bind to port 6379")
    os.Exit(1)
  }
	 
  for {
    conn, err := l.Accept()
    if err != nil {
      fmt.Println("Error accepting connection: ", err.Error())
      os.Exit(1)
    }

    go handleConnection(conn)
  }
}
