package fdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"

	"github.com/cespare/xxhash/v2"
	"github.com/vmihailenco/msgpack/v5"
)

var conn net.Conn

// Connects to the database server
//
// Returns error same as https://pkg.go.dev/net#Dial
func Connect(ip string, port uint) (err error) {
	addr := ip + ":" + strconv.Itoa(int(port))
	conn, err = net.Dial("tcp", addr)
	return
}

func generateOperand(url string, tokens []string, metadatas map[string]string) ([]byte, error) {
	var operandBuffer bytes.Buffer
	//We have to generate the operand in the given order --> Key0  23  2  url0  3  2  PM0  3  23  TOKEN[0]0  3  TOKEN[1]0  3  TOKEN[2]0 . . . . . n

	// Generating Key (8 bytes) - Corrected byte order
	key := make([]byte, 8)
	xxh := xxhash.Sum64String(url)
	binary.LittleEndian.PutUint64(key, xxh) // Use LittleEndian

	operandBuffer.Write(key)
	operandBuffer.WriteByte(23) // End of Transaction Block after key

	// Writing URL with Start and End of Text (2, 3)
	operandBuffer.WriteByte(2)
	operandBuffer.WriteString(url)
	operandBuffer.WriteByte(3)

	// Writing Metadata with Start and End of Text (2, 3)
	operandBuffer.WriteByte(2)
	packedMetadata, err := msgpack.Marshal(metadatas)
	if err != nil {
		return nil, fmt.Errorf("error packing metadata: %w", err)
	}
	operandBuffer.Write(packedMetadata)
	operandBuffer.WriteByte(3)
	operandBuffer.WriteByte(23)

	// Writing Tokens separated by Start and End of Text (2, 3)
	for _, token := range tokens {
		operandBuffer.WriteString(token)
		operandBuffer.WriteByte(3)
	}
	operandBuffer.WriteByte(23) // End of Transaction Block after tokens

	return operandBuffer.Bytes(), nil
}

func parseOperand(operand []byte) (string, map[string]string, []string, error) {
	operandBuffer := bytes.NewBuffer(operand)

	// ---------Extract the key (first 8 bytes)---------
	key := make([]byte, 8)
	_, err := operandBuffer.Read(key)
	if err != nil {
		return "", nil, nil, fmt.Errorf("error reading key: %w", err)
	}

	// Skip End of Transaction Block (23) after the key
	b, err := operandBuffer.ReadByte()
	if err != nil || b != 23 {
		return "", nil, nil, fmt.Errorf("expected End of Transaction Block after key")
	}

	//----------- Extracting the URL--------------
	b, err = operandBuffer.ReadByte()
	if err != nil || b != 2 {
		return "", nil, nil, fmt.Errorf("expected Start of Text for URL")
	}
	var urlBytes []byte
	for {
		b, err = operandBuffer.ReadByte()
		if err != nil || b == 3 {
			break
		}
		urlBytes = append(urlBytes, b)
	}
	url := string(urlBytes)

	// -------------- Extract the metadata (messagepack format) -------
	b, err = operandBuffer.ReadByte()
	if err != nil || b != 2 {
		return "", nil, nil, fmt.Errorf("expected Start of Text for metadata")
	}
	var packedMetadata []byte
	for {
		b, err = operandBuffer.ReadByte()
		if err != nil || b == 3 {
			break
		}
		packedMetadata = append(packedMetadata, b)
	}

	// Unmarshal the metadata
	var metadata map[string]string
	err = msgpack.Unmarshal(packedMetadata, &metadata)
	if err != nil {
		return "", nil, nil, fmt.Errorf("error unpacking metadata: %w", err)
	}

	// Skip End of Transaction Block (23) after the metadata
	b, err = operandBuffer.ReadByte() // Important: Read the 23 here
	if err != nil || b != 23 {
		return "", nil, nil, fmt.Errorf("expected End of Transaction Block after metadata")
	}

	//------------- Extract tokens-----------------
	var tokens []string
	for {
		b, err = operandBuffer.ReadByte()
		if err != nil {
			if err == io.EOF {
				return url, metadata, tokens, nil
			}
			return "", nil, nil, fmt.Errorf("error reading byte during token extraction: %w", err)
		}
		if b == 23 {
			return url, metadata, tokens, nil
		}
		var tokenBytes []byte
		for {
			if err != nil || b == 3 {
				break
			}
			tokenBytes = append(tokenBytes, b)
			b, err = operandBuffer.ReadByte()
		}
		tokens = append(tokens, string(tokenBytes))
	}
}

// Writes the data on the database
func Write(url string, tokens []string, metadatas map[string]string) (err error) {
	operandBytes, err := generateOperand(url, tokens, metadatas)
	if err != nil {
		return err
	}
	_, err = conn.Write(operandBytes) // Write the generated operand to the database
	return err
}

// Read the value of the token from the database
func Read(token string) (response []byte, err error) {
	byteobj := []byte{10}
	byteobj = append(byteobj, []byte(token)...)
	_, err = conn.Write(byteobj)
	if err != nil {
		return
	}

	buffer := make([]byte, 100)
	n, err := conn.Read(buffer)
	if err != nil {
		return
	}

	return buffer[:n], nil
}

func Close() {
	if conn != nil {
		defer conn.Close()
	}
}
