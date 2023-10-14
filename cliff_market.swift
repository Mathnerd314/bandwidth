import Foundation

// nix-shell --extra-experimental-features flakes -I nixpkgs=flake:github:nixos/nixpkgs/0218941ea68b4c625533bead7bbb94ccce52dceb -p swift 'linuxPackages_6_1.perf' flamegraph hyperfine
// swiftc -g -O -Xcc -march=native -Xcc -mtune=native --enforce-exclusivity=none cliff_market.swift
// sudo perf record -F 99 --call-graph dwarf -- ./cliff_market ANON2.csv
// sudo perf script > out.perf; stackcollapse-perf.pl out.perf | swift demangle > out.folded; flamegraph.pl out.folded > out.svg

// swiftc -emit-ir -g -O -module-name "x" --enforce-exclusivity=none cliff_market.swift > cliff_market.ll
// cat cliff_market.ll | swift demangle > cliff_market_demangled.ll

private enum State {
    case EOF
    case Line
    case Normal
}

private struct Product {
  var cnt : Int // Count of records
  var buys : Int // Buy transactions
  var sells : Int  // Sell transactions
  var tot_qty : Int; // Just the max qtys, totaled for an average
}

private func getInstanceOrCreateNew(dictionary: inout [String: Product], forKey key: String) -> Product {
  if let existingInstance = dictionary[key] {
    return existingInstance
  } else {
    let newInstance = Product(cnt: 0, buys: 0, sells: 0, tot_qty: 0)
    dictionary[key] = newInstance
    return newInstance
  }
}

// Assumption: columns appear in this order in the CSV (mixed in with the other fields)
let columns = ["Source","B/S","OrdQty","WrkQty","ExcQty","Prod"].map { s in [UInt8](s.utf8) }
let newline = "\n".utf8.first!
let comma = ",".utf8.first!
let chunkSize: Int = 32*1024

private func main() {
  var myDictionary = [String: Product]()
  let fileHandle = FileHandle(forReadingAtPath: CommandLine.arguments[1])!
  var data = try! [UInt8](fileHandle.read(upToCount: chunkSize)!)
  let eol_idx = data.firstIndex(of: newline)!
  let headers = data[0..<eol_idx].split(separator: comma, omittingEmptySubsequences: false)
  // print(headers.map { s in String(data: s, encoding: .utf8) })
  let indexes = columns.map { column in
    headers.firstIndex(of: column[...])!
  }
  let (idx_src, idx_bs, idx_ordqty, idx_wrkqty, idx_excqty, idx_prod) =
    (indexes[0],indexes[1],indexes[2],indexes[3],indexes[4],indexes[5])
  data = [UInt8](data[(eol_idx+1)..<data.count])

/*
Tests:
splitCells "x" = [EOF "x"]
splitCells ",," = [Normal "", Normal "", EOF ""]
splitCells "x,y" = [Normal "x", EOF "y"]
splitCells "x,y,z" = [Normal "x", Normal "y", EOF "z"]
splitCells "a,b\nc,d" = [Normal "d", Line "b," Normal "c", EOF "d"]

Haskell implementation:
splitCells "" = [EOF ""]
splitCells (',':xs) = [Normal ""] ++ splitCells xs
splitCells ('\n':xs) = [Line ""] ++ splitCells xs
splitCells (x:xs) = case splitCells xs of r:rs -> (x:r) : rs
*/

  var state = State.Normal
  var cur_pos = 0
  // in-row variables
  var col_idx = 0
  var in_real_row = false
  var buy = false
  var sell = false
  var prodNm = ""
  var tot_qty = 0

  while state != State.EOF {
    let prevstate = state
    // find extent of cell
    var cell_start_pos = cur_pos
    while true {
      if cur_pos >= data.count {
        let nextData = try! fileHandle.read(upToCount: chunkSize)
        if let nD = nextData, !nD.isEmpty {
          // Allocate new buffer with current (incomplete) segment + new chunk.
          data = [UInt8](data[cell_start_pos..<data.count])
          data.append(contentsOf: nD)
          (cell_start_pos, cur_pos) = (0, cur_pos - cell_start_pos)
        } else {
          // At EOF - set to buffer end
          cur_pos = data.count
          state = State.EOF
          break
        }
      } else if data[cur_pos] == comma {
        state = State.Normal
        break
      } else if data[cur_pos] == newline {
        state = State.Line
        break
      } else {
        cur_pos += 1
      }
    }
    // End of cell not including comma/newline
    let cell_data_end = cur_pos
    // End of cell including comma/newline (exclusive, so index of comma/newline plus one)
    var cell_span_end = cur_pos+1
    if state == State.EOF {
      cell_span_end = cur_pos
    }
    // print("found cell",String(data: data[cell_start_pos..<cell_span_end], encoding: .utf8)!)

    // Handle newline-eof case
    if prevstate == State.Line && state == State.EOF && cell_start_pos == cell_data_end {
      break
    }

    // Process cell
    let raw_col = data[cell_start_pos..<cell_data_end]
    if(col_idx == idx_src ) {
      in_real_row = raw_col == [UInt8]("ToClnt".utf8)[...]
    }
    if(in_real_row) {
      if(col_idx == idx_bs) {
        // Count buys and sells
        if raw_col == [UInt8]("Buy".utf8)[...] { buy = true }
        if raw_col == [UInt8]("Sell".utf8)[...] { sell = true }
      }
      if(col_idx == idx_prod) {
        prodNm = String(data: Data(raw_col), encoding: .utf8)!
      }
      if(col_idx == idx_ordqty || col_idx == idx_wrkqty || col_idx == idx_excqty) {
        tot_qty = [tot_qty, Int(String(data: Data(raw_col), encoding: .utf8)!)!].max()!
      }
    }

    // process product at end of row
    if(in_real_row && (state == State.EOF || state == State.Line)) {
      var prod = getInstanceOrCreateNew(dictionary: &myDictionary, forKey: prodNm)
      // Count actions
      prod.cnt += 1
      // Count buys and sells
      if buy { prod.buys += 1 }
      if sell { prod.sells += 1 }
      // Add max order/work/exec qty
      prod.tot_qty += tot_qty
      myDictionary[prodNm] = prod

      // Reset in-row variables
      in_real_row = false
      buy = false
      sell = false
      prodNm = ""
      tot_qty = 0
    }

    // Update where we are
    if (state == State.EOF || state == State.Line) {
      // print("columns:",state,col_idx)
      assert(col_idx == 61 /* headers.count */ , "CSV must be rectangular")
      // "Remaining fields" contains commas so the header does not match the rest of the file
      col_idx = 0
    } else if (state == State.Normal) {
      col_idx += 1
    } else {
      fatalError("unhandled case")
    }
    cur_pos = cell_span_end
  }

  // Print out orders
  for (name, prod) in myDictionary {
    print(String(format: "%@ %d buy=%d sell=%d avg qty=%6.2f", name, prod.cnt, prod.buys, prod.sells,
      Double(prod.tot_qty) / Double(prod.cnt)))
  }
}
main()
