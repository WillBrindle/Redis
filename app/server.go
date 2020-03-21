package main

import (
  "fmt";
  "os";
  "net";
  "io";
  "bytes";
  "strconv";
  "strings";
  "time"
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

      fmt.Println("Got command ", strArr)

      switch cmd := strings.ToUpper(strArr[0]); cmd {
      case "PING":
        conn.Write([]byte("+PONG\r\n"))
      case "ECHO":
        conn.Write([]byte("+" + strArr[1] + "\r\n"))
      case "COMMAND":
        conn.Write([]byte("+OK\r\n"))
      case "SET":
        var expTime int64 = -1
        for i := 3; i < elems; i += 2 {
          key := strArr[i]
          if strings.ToUpper(key) == "PX" {
            px, _ := strconv.ParseInt(strArr[i + 1], 10, 64)
            expTime = time.Now().Unix() * 1000 + px
          }
        }

        m[strArr[1]] = RedisRecord{strArr[2], expTime}
        conn.Write([]byte("+OK\r\n"))
      case "GET":
        now := time.Now().Unix() * 1000
        res, ok := m[strArr[1]]

        if ok == false {
          fmt.Println("Returning nil res")
          conn.Write([]byte("$-1\r\n"))
          continue
        }
        
        fmt.Println("Time is", now)
        fmt.Println("Expiry is", res.expiryTime)

        if res.expiryTime >= 0 && res.expiryTime <= now {
          fmt.Println("Returning expired res")
          conn.Write([]byte("$-1\r\n"))
          delete(m, strArr[1])
          continue
        }

        byteLen := len(res.value)
        formattedRes := "$" + strconv.Itoa(byteLen) + "\r\n" + res.value + "\r\n"
        fmt.Println("Returning ", formattedRes)
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
