// SPDX-License-Identifier: MIT
pragma solidity ^0.7.4;

contract StringSaver {
    string saved;

    constructor() {
        saved = "";
    }
    
    function get() public view returns (string memory) {
        return saved;
    }
    
    function set(string calldata val) public {
        saved = val;
    }
}
