#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::export]]
char isbn_checkdigit(std::string isbn) {
    if (isbn.size() == 10) {
        int wsum = 0;
        for (int i = 0; i < 9; i++) {
            int weight = 10 - i;
            char c = isbn[i];
            int digit = c - '0';
            if (digit < 0 || digit > 9) {
                stop("'isbn' must only have digits");
            }
            wsum += digit * weight;
        }
        int cks = (11 - wsum % 11) % 11;
        return cks == 10 ? 'X' : '0' + cks;
    } else if (isbn.size() == 13) {
        int wsum;
        for (int i = 0; i < 12; i++) {
            int weight = i % 2 ? 3 : 1;
            char c = isbn[i];
            int digit = c - '0';
            if (digit < 0 || digit > 9) {
                stop("'isbn' must only have digits");
            }
            wsum += weight * digit;
        }
        int mod = 10 - (wsum % 10);
        return mod == 10 ? '0' : ('0' + mod);
    } else {
        stop("'isbn' must have length 10 or 13");
    }
}

bool is_valid_isbn(std::string isbn) {
    size_t len = isbn.size();
    if (len == 10) {
        int wsum = 0;
        for (int j = 0; j < len; j++) {
            char c = isbn[j];
            int digit = c == 'X' ? 10 : c - '0';
            if (c < 0 || c > 10) return false;
            wsum += digit * (10 - j);
        }
        return (wsum % 11) == 0;
    } else if (len == 13) {
        int wsum = 0;
        for (int j = 0; j < len; j++) {
            char c = isbn[j];
            int digit = c - '0';
            if (c < 0 || c > 9) return false;
            wsum += digit * (j % 2 ? 3 : 1);
        }
        return (wsum % 10) == 0;
    } else {
        return false;
    }
}

// [[Rcpp::export]]
LogicalVector is_valid_isbn(CharacterVector isbns) {
    int n = isbns.size();
    LogicalVector out(n);
    for (int i = 0; i < n; i++) {
        String ri = isbns[i];
        std::string isbn = ri;
        out[i] = is_valid_isbn(isbn);
    }
    return out;
}