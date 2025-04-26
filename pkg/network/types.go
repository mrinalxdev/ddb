package network

import "encoding/json"

// TODO : type for network request
type Request struct {
	Op string `json:"op"`
	Key string `json:"key"`
	Value string `json:"value"` 
}

// TODO : network response

type Response struct {
	Status string `json:"status"`
	Value string `json:"value"`
	Error string `json:"error"`
}

// TODO : function to encode a req to json

func MarshalRequest(req *Request) ([]byte, error){
	return json.Marshal(req)

}
// TODO : dedcodes the json req
func UnMarshalRequest(data []byte)(*Request, error){
 var resp Response
 if err := json.UnMarshalRequest(data, &resp); err != nil {
	 return nil, err
 }

 return &resp, nil
}
