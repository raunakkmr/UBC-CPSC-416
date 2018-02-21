/*
Implements the solution to assignment 1 for UBC CS 416 2017 W2.

Usage:
$ go run client.go [local UDP ip:port] [local TCP ip:port] [aserver UDP ip:port]

Example:
$ go run client.go 127.0.0.1:2020 127.0.0.1:3030 127.0.0.1:7070

*/

package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"runtime"
	"strconv"
)

/////////// Msgs used by both auth and fortune servers:

// An error message from the server.
type ErrMessage struct {
	Error string
}

/////////// Auth server msgs:

// Message containing a nonce from auth-server.
type NonceMessage struct {
	Nonce string
	N     int64 // PoW difficulty: number of zeroes expected at end of md5(nonce+secret)
}

// Message containing an the secret value from client to auth-server.
type SecretMessage struct {
	Secret string
}

// Message with details for contacting the fortune-server.
type FortuneInfoMessage struct {
	FortuneServer string // TCP ip:port for contacting the fserver
	FortuneNonce  int64
}

/////////// Fortune server msgs:

// Message requesting a fortune from the fortune-server.
type FortuneReqMessage struct {
	FortuneNonce int64
}

// Response from the fortune-server containing the fortune.
type FortuneMessage struct {
	Fortune string
	Rank    int64 // Rank of this client solution
}

// Main workhorse method.
func main() {

	// Read the command line arguments.
	var args []string = os.Args[1:]
	if len(args) != 3 {
		fmt.Println("Usage: client.go [local UDP ip:port] [local TCP ip:port] [aserver UDP ip:port]")
		return
	}

	localUDP := args[0]
	localTCP := args[1]
	aserverUDP := args[2]

	// Resolve the local and aserver UDP addresses.
	localUDPAddr, _ := net.ResolveUDPAddr("udp", localUDP)
	aserverUDPAddr, _ := net.ResolveUDPAddr("udp", aserverUDP)

	// Create a UDP connection to the aserver.
	aserverConn, _ := net.DialUDP("udp", localUDPAddr, aserverUDPAddr)
	defer aserverConn.Close()

	// Send a UDP message with arbitrary payload to the aserver.
	message, _ := json.Marshal( /*arbitraryPayload=*/ 1)
	_, _ = aserverConn.Write(message)

	// Receive a NonceMessage reply containing the nonce and n.
	buf := make([]byte, 1024)
	var nonceMessage NonceMessage
	n, _ := aserverConn.Read(buf[0:])
	_ = json.Unmarshal(buf[0:n], &nonceMessage)

	// Find a string value, secret, such that the MD5 hash of concat(nonce,
	// secret) has n trailing zeros.
	// secret := computeSecret(nonceMessage.Nonce, nonceMessage.N)
	secretChan := make(chan string)
	doneChan := make(chan bool)
	num := int64(2 * runtime.NumCPU())
	num = int64(nonceMessage.N * nonceMessage.N * nonceMessage.N)
	block := int64(math.MaxInt64 / num)
	for i := int64(0); i < num; i++ {
		go computeSecret(nonceMessage.Nonce, nonceMessage.N, i*block, (i+1)*block, secretChan, doneChan)
	}
	secret := <-secretChan
	doneChan <- true

	// Send the secret value to the aserver as part of a SecretMessage.
	// secretMessage := SecretMessage{
	// 	Secret: secret,
	// }
	message, _ = json.Marshal(SecretMessage{
		Secret: secret,
	})
	_, _ = aserverConn.Write(message)

	// aserver verifies the client's PoW, and replies with a FortuneInfoMessage
	// that contains the TCP IP:port and the nonce to use to connect to the
	// fserver.
	var fortuneInfoMessage FortuneInfoMessage
	n, _ = aserverConn.Read(buf[0:])
	_ = json.Unmarshal(buf[0:n], &fortuneInfoMessage)

	// Resolve the local and fserver TCP addresses.
	localTCPAddr, _ := net.ResolveTCPAddr("tcp", localTCP)
	fserverTCPAddr, _ := net.ResolveTCPAddr("tcp", fortuneInfoMessage.FortuneServer)

	// Create a TCP connection to the fserver.
	fserverConn, _ := net.DialTCP("tcp", localTCPAddr, fserverTCPAddr)
	defer fserverConn.Close()

	// Send a FortuneReqMessage to fserver.
	// fortuneReqMessage := FortuneReqMessage{
	// 	FortuneNonce: fortuneInfoMessage.FortuneNonce,
	// }
	message, _ = json.Marshal(FortuneReqMessage{
		FortuneNonce: fortuneInfoMessage.FortuneNonce,
	})
	_, _ = fserverConn.Write(message)

	// Receive a FortuneMessage, which contains the fortune and the solution-rank.
	var fortuneMessage FortuneMessage
	n, _ = fserverConn.Read(buf[0:])
	_ = json.Unmarshal(buf[0:n], &fortuneMessage)

	// Print out the recieved fortune on a new newline-terminated line and then
	// exit.
	// fmt.Println(fortuneMessage.Rank)
	fmt.Println(fortuneMessage.Fortune)
}

// Returns the MD5 hash as a hex string for the (nonce + secret) value.
func computeNonceSecretHash(nonce string, secret string) string {
	h := md5.New()
	h.Write([]byte(nonce + secret))
	str := hex.EncodeToString(h.Sum(nil))
	return str
}

// Returns a string, secret, such that the MD5 hash of (nonce + secret) has n
// trailing zeros.
func computeSecret(nonce string, n int64, lo int64, hi int64, secret chan string, done chan bool) {
	val := ""
	var i int64 = 0
	zeros := ""
	for idx := int64(0); idx < n; idx++ {
		zeros = zeros + string('0')
	}
OuterLoop:
	for i = lo; i < hi; i++ {
		select {
		case <-done:
			return
		default:
			val = computeNonceSecretHash(nonce, strconv.FormatInt(i, 10))
			ok := false
			ok = val[int64(len(val))-n:] == zeros
			if ok {
				break OuterLoop
			}
		}
	}
	secret <- strconv.FormatInt(i, 10)
}

// func computeSecret(nonce string, n int64) string {
// 	val := ""
// 	var i int64 = 0
// 	zeros := ""
// 	for idx := int64(0); idx < n; idx++ {
// 		zeros = zeros + string('0')
// 	}
// OuterLoop:
// 	for {
// 		val = computeNonceSecretHash(nonce, strconv.FormatInt(i, 10))
// 		ok := false
// 		ok = val[int64(len(val))-n:] == zeros
// 		if ok {
// 			break OuterLoop
// 		}
// 		i++
// 	}
// 	return strconv.FormatInt(i, 10)
// }
