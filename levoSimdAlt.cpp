/*Compile with
g++ -std=c++11 -fno-exceptions -fno-rtti -march=native -mtune=native -Wall -Wextra -g -O2 main.cpp
*/
#include<cstdio>
#include<cstdlib>
#include<cstring>
#include<cassert>
#include<unordered_map>
#include<algorithm>
#include<immintrin.h>

typedef unsigned int u32;
typedef unsigned long long u64;

struct ProdData {
	int buys, sells;
	long long cnt, totalQty;
};

int ParseInt(const char*sz, u64*out)
{
	if (!sz) return -1;
	u64 val=0; //overflow is legal with unsigned
	int i=0;
	while (1)
	{
		char c = sz[i];
		if ((unsigned)(c-'0') > 9u) break;
		u64 prevVal = val;
		val = val*10+c-'0';
		if (val <= prevVal && val != 0) {
			return -1;
		}
		i++;
	}
	if (i == 0)
		return -1;
	*out = val;
	return i;
}

int process(FILE*f) {
	auto buflen = 2<<20;
	auto buf = (char*)malloc(buflen+64); //memory leak
	buf += ((u64)buf & 31) == 0 ? 0 : 32-(((u64)buf & 31)); //align to 32bytes for simd

	fgets(buf, buflen, f);
/*
"Source"
"B/S", "Prod"
OrdQty,WrkQty,ExcQty
*/
	int SourceIndex=-1, BuySellIndex=-1, ProdIndex=-1, OrdQtyIndex=-1, WrkQtyIndex=-1, ExcQtyIndex=-1, headerCount=0, n=0, columnIndex=0;
	while (1)
	{
		if (buf[n] == '\n')
			break;
		if (buf[n] == 0) {
			fprintf(stderr, "Bad file, did not found the columns expected\n");
			return 1;
		}
		int match=1;
		const char*tryWord;
		int*pIndex;
		if (buf[n] == 'S') {
			tryWord = "Source";
			pIndex = &SourceIndex;
		} else if (buf[n] == 'B') {
			tryWord = "B/S";
			pIndex = &BuySellIndex;
		} else if (buf[n] == 'P') {
			tryWord = "Prod";
			pIndex = &ProdIndex;
		} else if (buf[n] == 'O') {
			tryWord = "OrdQty";
			pIndex = &OrdQtyIndex;
		} else if (buf[n] == 'W') {
			tryWord = "WrkQty";
			pIndex = &WrkQtyIndex;
		} else if (buf[n] == 'E') {
			tryWord = "ExcQty";
			pIndex = &ExcQtyIndex;
		} else {
			match = 0;
		}
		if (match) {
			++n;
			for(int i=1; tryWord[i]; i++, n++) {
				if (tryWord[i] != buf[n]) {
					match=0;
					break;
				}
			}
			if (match && (buf[n] == ',' || buf[n] == '\n' || buf[n] == 0)) {
				if (*pIndex != -1) {
					fprintf(stderr, "Duplicate Column %s\n", tryWord);
					return 1;
				}
				*pIndex = columnIndex;
				headerCount++;
				columnIndex++;
				n++;
				continue;
			}
		}
		char c;
		while (1) {
			c = buf[++n];
			if (c == ',' || c == '\n' || c == 0)
				break;
		}
		columnIndex++;
		if (c == ',') { ++n; }
	}
	if (headerCount != 6) {
		fprintf(stderr, "Invalid Columns\n");
		return 1;
	}
	
	const char validHeader[] = "RecordNo,Date/Time,Exch,SrsKey,Source,Status,OrderNo,ExchOrderId,Action,B/S,OrdQty,WrkQty,ExcQty,Prod,Expiry,O/C,C/P,LimitPrc,StopPrc,Strike,OrderType,OrderRes,ExchMember,ExchGroup,ExchTrader,User ID,Member,Group,Trader,Account,FFT1,FFT2,FFT3,ClrMember,ExchTime,ExchDate,Srvr,TxtMsg,GW Specific,Remaining Fields";
	if (memcmp(buf, validHeader, sizeof(validHeader)-1) != 0) {
		fprintf(stderr, "Header wasn't the exact string expected\n");
		return 1;
	}

	const int maxInterestedColumn = columnIndex;
	std::unordered_map<int, ProdData> map;
	int row=0;
	auto blockCommas = _mm256_set1_epi8(',');
	int commas[maxInterestedColumn+64];
	commas[0] = 0;
	while (fgets(buf, buflen, f))
	{
		int offset=0, commaIndex=1;
		while(commaIndex<=maxInterestedColumn)
		{
			auto blockA = _mm256_load_si256((__m256i*)(buf+offset));
			auto blockB = _mm256_load_si256((__m256i*)(buf+offset+32));

			auto commaA = _mm256_cmpeq_epi8(blockA, blockCommas);
			auto commaB = _mm256_cmpeq_epi8(blockB, blockCommas);

			auto tzA = (u32)_mm256_movemask_epi8(commaA);
			auto tzB = (u32)_mm256_movemask_epi8(commaB);
			{
				while(tzA) {
					int i = _tzcnt_u64(tzA);
					//tzA ^= 1<<i;
					tzA &= tzA-1;
					commas[commaIndex++] = offset+i+1;
				}
				while(tzB) {
					int i = _tzcnt_u64(tzB);
					//tzB ^= 1<<i;
					tzB &= tzB-1;
					commas[commaIndex++] = offset+32+i+1;
				}
			}
			offset += 64;
		}

		row++;
		if (memcmp(buf+commas[SourceIndex], "ToClnt", 6) != 0)
			continue;
		int productName;
		assert((buf+commas[ProdIndex])[3] == ',');
		(buf+commas[ProdIndex])[3] = 0;
		memcpy(&productName, buf+commas[ProdIndex], 4);
		auto &prod = map[productName];
		prod.cnt++;
		if (memcmp(buf+commas[BuySellIndex], "Buy", 3) == 0)
			prod.buys++;
		else if (memcmp(buf+commas[BuySellIndex], "Sell", 4) == 0)
			prod.sells++;
		else {
			fprintf(stderr, "Row %d incorrect buy/sell data\n", row);
			return 1;
		}
		{
			u64 a, b, c;
			ParseInt(buf+commas[OrdQtyIndex], &a);
			ParseInt(buf+commas[WrkQtyIndex], &b);
			ParseInt(buf+commas[ExcQtyIndex], &c);
			prod.totalQty += std::max(a, std::max(b, c));
		}
	}

	printf("{");
	for (auto kv : map) {
		auto&p=kv.second;
		printf("%s buy=%d sell=%d avg qty=%6.2f, ", (char*)&kv.first, p.buys, p.sells,(double)p.totalQty/p.cnt);
	}
	printf("}\n");
	return 0;
}

int main()
{
	FILE*f=fopen("/tmp/ANON2.csv", "rb");
	int ret = process(f);
	fclose(f);
	return ret;
}
