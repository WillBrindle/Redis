package main

import (
  "fmt";
  "os";
  "net";
  "io";
  "bytes";
  "strconv";
  "strings"
)

var m map[string]RedisRecord

type RedisRecord struct {
  value string
  expiryTime int64
}

func readChunk(b *bytes.Buffer) []byte {
  c := make([]byte, 0, b.Len())
  read := 0
  for {
    p := b.Bytes()
    if bytes.Equal(p[:2], []byte("\r\n")) {
      break
    }
    c = append(c, b.Next(1)...)
    read ++
  }

  b.Next(2)

  return c[0:read]
}

func handleConnection(conn net.Conn) {
  fmt.Println("Connection accepted")

  tmp := make([]byte, 1024)
  for {
    _, err := conn.Read(tmp)
    if err != nil {
      if err != io.EOF {
        fmt.Println("read error:", err)
      }
      break
    }

    b := bytes.NewBuffer(tmp)
    msgStart := readChunk(b)

    if msgStart[0] == byte('*') {
      elems, err := strconv.Atoi(string(msgStart[1:]))
      if err != nil {
        fmt.Println("Conversion error")
        break
      }

      strArr := make([]string, elems)
      for i := 0; i < elems; i++ {
        // TODO: handle long commands & overflows; will need length for that but can ignore for now
        readChunk(b)
        strArr[i] = string(readChunk(b))
      }

      fmt.Println("Got command ", strArr[0])

      switch cmd := strings.ToUpper(strArr[0]); cmd {
      case "PING":
        conn.Write([]byte("+PONG\r\n"))
      case "ECHO":
        conn.Write([]byte("+" + strArr[1] + "\r\n"))
      case "COMMAND":
        conn.Write([]byte("+OK\r\n"))
      case "SET":
        m[strArr[1]] = RedisRecord{strArr[2], -1}
        conn.Write([]byte("+OK\r\n"))
      case "GET":
        res, ok := m[strArr[1]]
        if ok == false {
          conn.Write([]byte("$-1\r\n"))
          continue
        }
        byteLen := len(res.value)
        formattedRes := "$" + strconv.Itoa(byteLen) + "\r\n" + res.value + "\r\n"
        conn.Write([]byte(formattedRes))
      }
    }
  }

  conn.Close()
}

func main() {
  fmt.Println("Launching server!")

  m = make(map[string]RedisRecord)

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
