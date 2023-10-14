#include<cstdio>
#include<cstdlib>
#include<cstring>
#include<cassert>
#include<unordered_map>
#include<algorithm>

//From Header String
enum {
	RecordNo, DateTime,Exch,SrsKey,Source,Status,OrderNo,ExchOrderId,Action,BuySell,OrdQty,WrkQty,ExcQty,Prod,HeaderEnd
};

struct ProdData {
	int product, buys, sells;
	long long cnt, totalQty;
};
//Buy Sell

int process(FILE*f) {
	auto buflen = 2<<20;
	auto buf = (char*)malloc(buflen);
	fgets(buf, buflen, f);
	const char validHeader[] = "RecordNo,Date/Time,Exch,SrsKey,Source,Status,OrderNo,ExchOrderId,Action,B/S,OrdQty,WrkQty,ExcQty,Prod,Expiry,O/C,C/P,LimitPrc,StopPrc,Strike,OrderType,OrderRes,ExchMember,ExchGroup,ExchTrader,User ID,Member,Group,Trader,Account,FFT1,FFT2,FFT3,ClrMember,ExchTime,ExchDate,Srvr,TxtMsg,GW Specific,Remaining Fields";
	if (memcmp(buf, validHeader, sizeof(validHeader)-1) != 0) {
		fprintf(stderr, "Header wasn't the exact string expected\n");
		return 1;
	}

	std::unordered_map<int, ProdData> map;
	char*cols[HeaderEnd];
	int row=0;
	while (fgets(buf, buflen, f))
	{
		int col=0, pos=0, left=0;
		//parseLine
		while(buf[pos])
		{
			if (buf[pos] == ',') {
				buf[pos]=0;
				cols[col++] = buf+left;
				left = pos+1;
				if (col == HeaderEnd) {
					break;
				}
			}
			pos++;
		}
		if (row == 98) {
			int z=0;
		}
		row++;
		if (col != HeaderEnd) {
			fprintf(stderr, "Row %d is incorrect\n", row);
			return 1;
		}
		if (memcmp(cols[Source], "ToClnt", 6) != 0)
			continue;
		int productName=0;
		assert(cols[Prod][3] == 0);
		memcpy(&productName, cols[Prod], 4);
		auto &prod = map[productName];
		prod.cnt++;
		if (memcmp(cols[BuySell], "Buy", 3) == 0)
			prod.buys++;
		else if (memcmp(cols[BuySell], "Sell", 4) == 0)
			prod.sells++;
		else {
			fprintf(stderr, "Row %d incorrect buy/sell data\n", row);
			return 1;
		}
		{
			int a = atoi(cols[OrdQty]);
			int b = atoi(cols[WrkQty]);
			int c = atoi(cols[ExcQty]);
			prod.totalQty += std::max(a, std::max(b, c));
		}
	}
	//{AWV=AWV 1 buy=0 sell=1 avg qty=182.00, ARS=ARS 2 buy=0 sell=2 avg qty=134.00, AVZ=AVZ 1 buy=1 sell=0 avg qty= 82.00, ,meta=,meta 51 buy=0 sell=0 avg qty=1247.18, AEW=AEW 1 buy=0 sell=1 avg qty= 23.00, BEX=BEX 1 buy=1 sell=0 avg qty=  1.00, BDW=BDW 1 buy=0 sell=1 avg qty=  1.00, AXL=AXL 1 buy=1 sell=0 avg qty=539.00, AME=AME 1 buy=1 sell=0 avg qty=  1.00, ARK=ARK 1 buy=0 sell=1 avg qty=376.00, AWS=AWS 1 buy=0 sell=1 avg qty=430.00, AQO=AQO 1 buy=0 sell=1 avg qty= 20.00, AOM=AOM 1 buy=0 sell=1 avg qty=185.00}

	printf("{");
	for (auto kv : map) {
		//String.format("%3s %d buy=%d sell=%d avg qty=%6.2f",_product,_cnt,_buys,_sells,(double)_tot_qty/_cnt);
		auto&p=kv.second;
		printf("%s buy=%d sell=%d avg qty=%6.2f, ", (char*)&kv.first, p.buys, p.sells,(double)p.totalQty/p.cnt);
	}
	printf("}\n");
	return 0;
}

int main(int argc, char *argv[])
{
	FILE*f=fopen("./ANON2.csv", "rb");
	int ret = process(f);
	fclose(f);
	return ret;
}
