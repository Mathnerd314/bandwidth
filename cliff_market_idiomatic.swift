import Foundation

// nix-shell --extra-experimental-features flakes -I nixpkgs=flake:github:nixos/nixpkgs/0218941ea68b4c625533bead7bbb94ccce52dceb -p swift 'linuxPackages_6_1.perf' flamegraph


private final struct Product {
  var cnt : Int // Count of records
  var buys : Int // Buy transactions
  var sells : Int  // Sell transactions
  var tot_qty : Int; // Just the max qtys, totaled for an average
}

// https://github.com/RMJay/LineReader/blob/master/LineReader/LineReader.swift
private final class LineReader {

  let encoding: String.Encoding
  let chunkSize: Int
  var fileHandle: FileHandle
  let delimData: Data
  var buffer: Data
  var atEof: Bool

  public init(file: FileHandle, encoding: String.Encoding = .utf8, chunkSize: Int = 4096) throws {
    let fileHandle = file
    self.encoding = encoding
    self.chunkSize = chunkSize
    self.fileHandle = fileHandle
    self.delimData = "\n".data(using: encoding)!
    self.buffer = Data(capacity: chunkSize)
    self.atEof = false
  }

  /// Return next line, or nil on EOF.
  public func readLine() -> String? {
    // Read data chunks from file until a line delimiter is found:
    while !atEof {
      //get a data from the buffer up to the next delimiter
      if let range = buffer.range(of: delimData) {
        //convert data to a string
        let line = String(data: buffer.subdata(in: 0..<range.lowerBound), encoding: encoding)!
        //remove that data from the buffer
        buffer.removeSubrange(0..<range.upperBound)
        return line
      }

      let nextData = fileHandle.readData(ofLength: chunkSize)
      if !nextData.isEmpty {
        buffer.append(nextData)
      } else {
        //End of file or read error
        atEof = true
        if !buffer.isEmpty {
          // Buffer contains last line in file (not terminated by delimiter).
          let line = String(data: buffer as Data, encoding: encoding)!
          return line
        }
      }
    }
    return nil
  }
}

func getInstanceOrCreateNew(dictionary: inout [String: Product], forKey key: String) -> Product {
  if let existingInstance = dictionary[key] {
    return existingInstance
  } else {
    let newInstance = Product(cnt: 0, buys: 0, sells: 0, tot_qty: 0)
    dictionary[key] = newInstance
    return newInstance
  }
}

func main() {
  let lineReader = try! LineReader(file: FileHandle(forReadingAtPath: CommandLine.arguments[1])!)
  var myDictionary = [String: Product]()
  // Read the header line, and pick out the columns of interest
  let headers = lineReader.readLine()!.split(separator: ","); // Read header
  let idx_src    = headers.firstIndex(of: "Source")! // Index of Source column
  let idx_bs     = headers.firstIndex(of: "B/S"   )! // Index of Prod   column
  let idx_ordqty = headers.firstIndex(of: "OrdQty")! // Index of B/S    column
  let idx_wrkqty = headers.firstIndex(of: "WrkQty")! // Index of OrdQty column
  let idx_excqty = headers.firstIndex(of: "ExcQty")! // Index of WrkQty column
  let idx_prod   = headers.firstIndex(of: "Prod"  )! // Index of ExcQty column
  // For each product, count records
  while let wholeLine = lineReader.readLine() {
    let line = wholeLine.split(separator: ",");
    let source = line[idx_src];
    if source != "ToClnt" { continue } // Filter out this row
    let bs    = line[idx_bs    ];
    let ordqty = line[idx_ordqty];
    let wrkqty = line[idx_wrkqty];
    let excqty = line[idx_excqty];
    let prodNm = String(line[idx_prod]);
    var data = getInstanceOrCreateNew(dictionary: &myDictionary, forKey: prodNm)
    // Count actions
    data.cnt = data.cnt + 1
    // Count buys and sells
    if bs == "Buy" { data.buys = data.buys + 1 }
    if bs == "Sell" { data.sells = data.sells + 1 }
    // Count max order/work/exec qty
    if let ord = Int(ordqty), let wrk = Int(wrkqty), let exc = Int(excqty) {
      data.tot_qty = data.tot_qty + [ord, wrk, exc].max()!
    } else {
      print("One or more strings couldn't be converted to integers")
    }
    myDictionary[prodNm] = data
  }
  for (name, prod) in myDictionary {
    print(String(format: "%@ %d buy=%d sell=%d avg qty=%6.2f", name, prod.cnt, prod.buys, prod.sells,
      Double(prod.tot_qty) / Double(prod.cnt)))
  }
}
main()
