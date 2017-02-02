package smallhash

//import "fmt"
import "strings"
import "strconv"
import "math/big"
import "crypto/sha1"
import "encoding/hex"

const OFFSET_BASIS uint64 = 2166136261
const FNV_PRIME uint64 = 16777619

// A super simple, super silly approx 8-bit hash,
func ModHash(keyword string) uint64 {

	sum := 0
	//buckets := 16777619
	buckets := 251 // formerly: 61 for 2^6

	// Breaks up keyword into bytes in ASCII format and loops through the resulting byte array
	// The 'each' variable has been converted to ASCII
	for _, each := range keyword {
		//fmt.Println("each byte in keyword is: ", each)
		sum += int(each) // Cast to desired format
	}

	// check ref on better hash function
	result := sum % buckets

	return uint64(result)

}

// A super simple, super silly approx 8-bit hash,
func ModHash_4(keyword string) uint64 {

	sum := 0
	//buckets := 16777619
	buckets := 16 // formerly: 61 for 2^6

	// Breaks up keyword into bytes in ASCII format and loops through the resulting byte array
	// The 'each' variable has been converted to ASCII
	for _, each := range keyword {
		//fmt.Println("each byte in keyword is: ", each)
		sum += int(each) // Cast to desired format
	}

	// check ref on better hash function
	result := sum % buckets

	return uint64(result)

}

// A super simple, proof-of-concept hash the uses XOR
func XorHash(keyword string) uint64 {
	result := 0

	// Breaks up keyword into bytes in ASCII format and loops through the resulting byte array
	// The 'each' variable has been converted to ASCII
	for _, each := range keyword {
		//fmt.Println("each byte in keyword is: ", each)
		result ^= int(each) // Cast to desired format
	}

	return uint64(result)
}

func FnvHash(keyword string, bitlength int) uint64 {

	var hash uint64 = OFFSET_BASIS + uint64(bitlength)

	// should be an 8 bit unsigned integer...need to confirm this
	byteArray := []byte(keyword)

	for index, each := range byteArray {
		index += index // Just an index counter; not used just wanted the go compiler to shut up!

		hash = hash * FNV_PRIME
		hash = hash * uint64(each) // need to xor NOT *
	}

	return hash

}

func MiniHash(keyword string) uint64 {
	var P1 uint64 = 7
	var P2 uint64 = 31

	var hash uint64 = P1

	//for (const char* p = s; *p != 0; p++) {
	for _, each := range keyword {
		hash = hash*P2 + uint64(each)
	}
	return hash
}

/*
 * leftPad just repoeats the padStr the indicated number of times
 */
func LeftPad(value int, pad int, pLen int) string {
	valueStr := strconv.Itoa(value)
	padStr := strconv.Itoa(pad)
	return strings.Repeat(padStr, pLen) + valueStr
}

// Should return an 8-bit hash, very naive lookup algorithm
func Sha1ShortHash(keyword string) uint8 {
	binary := big.NewInt(0)

	// Convert to sha1 hash
	hash := sha1.New()
	hash.Write([]byte(keyword))

	//Encode the 160-bit hash (base 16: hex) to string
	hexstr := hex.EncodeToString(hash.Sum(nil))

	//Convert to regular integer
	binary.SetString(hexstr, 16)
	resultStr := binary.String()
	//fmt.Println(resultStr)
	resultInt, _ := strconv.Atoi(resultStr[0:2])
	return uint8(resultInt)
}
