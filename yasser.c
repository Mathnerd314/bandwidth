// NOT A SOLUTION (reads whole file in one go, OOM's)
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <sys/stat.h>

#ifdef _WIN32
#define fileno _fileno
#define fstat _fstat64
#define stat _stat64
#endif

typedef struct {
    size_t length;
    const uint8_t* data;
} NL_Slice;

static NL_Slice read_string(const char* path) {
    FILE* fp = fopen(path, "rb");
    int descriptor = fileno(fp);

    struct stat file_stats;
    if (fstat(descriptor, &file_stats) == -1) {
        fprintf(stderr, "error: failed to open %s", path);
        abort();
    }

    size_t length = file_stats.st_size;
    uint8_t* data = malloc(length);

    fseek(fp, 0, SEEK_SET);
    if (fread(data, 1, length, fp) == 0) {
        free(data);

        fprintf(stderr, "error: failed to open %s", path);
        abort();
    }
    fclose(fp);
    return (NL_Slice){ length, data };
}

typedef enum {
    COL_SOURCE,    // Source
    COL_BS,        // B/S
    COL_ORDER_QTY, // OrdQty
    COL_EXEC_QTY,  // ExcQty
    COL_WORK_QTY,  // WrkQty
    COL_PROD,      // Prod

    COL_MAX,
} Column;

typedef enum { FROM_HOST, FROM_CLIENT, TO_HOST, TO_CLIENT } Source;
typedef enum { BUY, SELL } BS;

static const char* col_names[] = {
    "Source", "B/S", "OrdQty", "ExcQty", "WorkQty", "Prod"
};

typedef struct {
    int64_t total;
    int entries;
    int buys;
    int sells;
} Product;

typedef struct {
    uint32_t name;
    Product  product;
} ProductEntry;

typedef struct {
    ProductEntry* entries_arr;
    uint64_t entries_len;
    uint64_t entries_cap;

    int64_t* hashes_arr;
    uint64_t hashes_len;
} ProductHash;

static uint64_t next_pow2(uint64_t x) {
    return 1 << (64 - __builtin_clzll(x - 1));
}

ProductHash ph_init(int64_t size) {
    ProductHash ph;

    ph.entries_cap = size;
    ph.entries_arr = calloc(sizeof(ProductEntry), size);
    ph.entries_len = 0;

    ph.hashes_len = next_pow2(size);
    ph.hashes_arr = malloc(sizeof(int64_t)*  ph.hashes_len);

    for (int64_t i = 0; i < ph.hashes_len; i++) {
        ph.hashes_arr[i] = -1;
    }

    return ph;
}

static uint32_t ph_hash(uint32_t name, int len) {
    uint64_t cmp_mask = ((1ull << (len * 8)) - 1ull);
    return ((name & cmp_mask) * 11400714819323198485ull) >> 32ull;
}

static void ph_update(ProductHash* ph, NL_Slice name, Product p) {
    if (name.length > 3) {
        printf("Product name too long!\n");
        return;
    }

    char internal_name[4] = {0};
    memcpy(internal_name, name.data, name.length);

    int32_t name_num =* (int32_t* )internal_name;
    int name_hash = ph_hash(name_num, name.length);

    uint64_t hv = ((uint64_t)name_hash) & (ph->hashes_len - 1);
    for (uint64_t i = 0; i < ph->hashes_len; i++) {
        uint64_t idx = (hv + i) & (ph->hashes_len - 1);

        int64_t e_idx = ph->hashes_arr[idx];
        if (e_idx == -1) {
            ProductEntry entry = {.name = name_num, .product = p};

            ph->hashes_arr[idx] = ph->entries_len;
            ph->entries_arr[ph->entries_len] = entry;
            ph->entries_len += 1;

            return;
        }

        if ((uint64_t)ph->entries_arr[e_idx].name == name_num) {
            Product* prod = &ph->entries_arr[e_idx].product;
            prod->entries += 1;
            prod->total += p.total;
            prod->buys += p.buys;
            prod->sells += p.sells;

            return;
        }
    }
}

// You ready for some bullshit?
// Skipping 8 characters at a time, searching for newlines!
static const uint64_t nl_mask = (~(uint64_t)0) / 255 * (uint64_t)('\n');
#define has_zero(x) (((x)-(uint64_t)(0x0101010101010101)) & ~(x)&(uint64_t)(0x8080808080808080))

static size_t skip_to_end(const char* str, size_t len, size_t curr) {
    uint64_t chunk;
    while (curr + 8 < len) {
        memcpy(&chunk, str + curr, sizeof(chunk));
        uint64_t xor = chunk ^ nl_mask;
        if (!has_zero(xor)) {
            curr += 8;
        } else {
            break;
        }
    }

    while (curr < len) {
        char ch = str[curr];
        if (ch == '\n') {
            break;
        }
        curr++;
    }

    end:
    return curr;
}

static size_t skip_to_next(const char* str, size_t len, size_t curr) {
    while (curr < len) {
        char ch = str[curr++];
        if (ch == ',' || ch == '\n') {
            break;
        }
    }
    return curr;
}

static void process(size_t len, const char str[]) {
    ProductHash pmap = ph_init(2048);

    int col_next_n = 0;
    int col_offsets[COL_MAX]; // numbers of commas to find a field.
    int col_order[COL_MAX]; // we wanna parse in order to avoid multiple passes over the line.
    int col_next[COL_MAX]; // sort these in a sensible order?

    // parse "syntax" from the first line
    size_t curr = 0, last_comma = 0, col_num = 0;
    while (curr < len) {
        char ch = str[curr++];
        if (ch == ',' || ch == '\n') {
            size_t col_len = curr - last_comma - 1;

            // carriage return... my favorite...
            if (curr > 0 && str[curr-2] == '\r') {
                col_len -= 1;
            }

            const char* col = &str[last_comma];

            // write if we see a relevant column, mark it
            Column c = COL_MAX;
            switch (col_len) {
                case 3: if (!memcmp(col, "B/S",    3)) { c = COL_BS;        } break;
                case 4: if (!memcmp(col, "Prod",   4)) { c = COL_PROD;      } break;
                case 6: if (!memcmp(col, "ExcQty", 6)) { c = COL_EXEC_QTY;  }
                else    if (!memcmp(col, "Source", 6)) { c = COL_SOURCE;    }
                else    if (!memcmp(col, "OrdQty", 6)) { c = COL_ORDER_QTY; }
                else    if (!memcmp(col, "WrkQty", 6)) { c = COL_WORK_QTY;  } break;
            }

            if (c != COL_MAX) {
                //printf("Col #%zu: %.*s\n", col_num, (int) col_len, col);
                col_order[col_next_n++] = c;
                col_offsets[c] = col_num;
            }

            last_comma = curr;
            col_num++;

            if (ch == '\n') {
                break;
            }
        }
    }

    for (int i = 0; i < COL_MAX; i++) {
        col_next[i] = col_offsets[col_order[i]];
    }

    // actually parse file now
    while (curr < len) retry: {
        // everything but the product name is converted into an integer compatible
        // value, for the quantities it's just a parsed int, for the Source and Buy/Sell
        // it's the BS
        NL_Slice product = { 0 };
        int parsed[COL_MAX];

        col_next_n = 0;
        col_num = 0;
        size_t line_start = curr;

        // parse entire line for relevant fields
        while (curr < len) {

            // skip until relevant column
            if (col_next_n >= COL_MAX) {
                curr = skip_to_end(str, len, curr);
                break;
            }

            int next_col = col_next[col_next_n];
            if (col_num != next_col) {
                curr = skip_to_next(str, len, curr);
                col_num++;
                char ch = str[curr];
                if (ch == '\n') {
                    break;
                }

                continue;
            }

            Column col = (Column)col_order[col_next_n];
            col_next_n++;

            size_t str_end = skip_to_next(str, len, curr);
            const char* col_str = str + curr;
            size_t col_len = str_end - curr - 1;
            curr = str_end;

            col_num++;

            int p = -1;
            switch (col) {
                case COL_PROD: {
                    product = (NL_Slice){ col_len, (const uint8_t*) col_str };
                } break;
                case COL_BS: {
                    if      (!memcmp(col_str, "Buy", 3))  { p = BUY;  }
                    else if (!memcmp(col_str, "Sell", 4)) { p = SELL; }
                } break;
                case COL_SOURCE: {
                    if (!memcmp(col_str, "ToClnt", 6)) { p = TO_CLIENT; }
                    else {
                        // skip line
                        curr = skip_to_end(str, len, curr) + 1;
                        if (curr < len) {
                            goto retry;
                        } else {
                            goto done;
                        }
                    }
                } break;
                default: {
                    p = 0;
                    for (size_t i = 0; i < col_len; i++) {
                        unsigned char c = col_str[i];
                        if (c < '0' || c > '9') break;

                        p = (p * 10) + (int)(c & 0xf);
                    }
                }
            }

            parsed[col] = p;
        }

        // skip the trailing \n and \r
        curr++;
        if (curr < len && str[curr] == '\r') {
            curr++;
        }

        // do the fun logic
        if (parsed[COL_SOURCE] == TO_CLIENT) {
            // > For each unique product, count Buys vs Sells. Take the Max of
            // > the 3 Qty columns, and total it, along with counting the number
            // > of entries.
            int m = parsed[COL_ORDER_QTY];
            if (m < parsed[COL_WORK_QTY]) m = parsed[COL_WORK_QTY];
            if (m < parsed[COL_EXEC_QTY]) m = parsed[COL_EXEC_QTY];

            bool is_buy = parsed[COL_BS] == BUY;
            Product p = { .entries = 1, .total = m, .buys = is_buy, .sells = !is_buy };
            ph_update(&pmap, product, p);
        }
    }

    done:
    for (int i = 0; i < pmap.entries_len; i++) {
        ProductEntry* p = &pmap.entries_arr[i];
        char* name = (char*) &p->name;
        double avg = (double) p->product.total / p->product.entries;
        printf(
            "%3s %d buy=%d sell=%d avg qty=%6.2f\n",
            name, p->product.entries, p->product.buys, p->product.sells, avg
        );
    }
}

int main(int argc, char** argv) {
    if (argc != 2) {
        printf("Invalid args for %s\n", argv[0]);
        return 1;
    }

    NL_Slice text = read_string(argv[1]);
    process(text.length, (const char*) text.data);

    return 0;
}
