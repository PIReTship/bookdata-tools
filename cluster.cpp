#include <Rcpp.h>
using namespace Rcpp;

#include <cmath>
#include <algorithm>
#include <unordered_map>
#include <iostream>
#include <chrono>

typedef std::chrono::steady_clock timer;

std::string time_since(const timer::time_point& start) {
    timer::time_point now = timer::now();
    timer::duration elapsed = now - start;
    double eticks = elapsed.count();
    eticks *= timer::duration::period::num;
    eticks /= timer::duration::period::den;
    
    char array[64];
    std::snprintf(array, 64, "[%8.2f] ", eticks);
    return std::string(array);
}

// [[Rcpp::export]]
DataFrame compute_clusters(DataFrame init_clusters, DataFrame edges)
{
    IntegerVector isbns = init_clusters["isbn_id"];
    IntegerVector init = init_clusters["cluster"];
    IntegerVector lefts = edges["left_isbn"];
    IntegerVector rights = edges["right_isbn"];
    
    std::unordered_map<int, int> cluster_map;
    int nisbns = isbns.length();
    int nedges = lefts.length();
    Function message("message");

    for (int i = 0; i < nisbns; i++) {
        int isbn = isbns[i];
        cluster_map[isbn] = init[i];
    }

    auto start = timer::now();
    int nchanged = nedges;
    int iter = 0;
    while (nchanged > 0) {
        nchanged = 0;
        iter += 1;
        message(time_since(start), "starting iteration ", iter);
        for (int i = 0; i < nedges; i++) {
            int left = lefts[i];
            int right = rights[i];
            int lc = cluster_map.at(left);
            int& rc = cluster_map.at(right);
            if (lc < rc) {
                rc = lc;
                nchanged += 1;
            }
        }

        message(time_since(start), "iteration ", iter, " changed ", nchanged, " memberships");
    }

    IntegerVector out(nisbns);
    for (int i = 0; i < nisbns; i++) {
        out[i] = cluster_map[isbns[i]];
    }
    return DataFrame::create(Named("isbn_id") = isbns, Named("cluster") = out);
}
