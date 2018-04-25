#include <Rcpp.h>
using namespace Rcpp;

#include <cmath>
#include <algorithm>
#include <unordered_map>
#include <iostream>

// [[Rcpp::export]]
IntegerVector compute_clusters(IntegerVector isbns, IntegerVector init, IntegerVector lefts, IntegerVector rights)
{
    std::unordered_map<int, int> cluster_map;
    int nisbns = isbns.length();
    int nedges = lefts.length();

    for (int i = 0; i < nisbns; i++) {
        int isbn = isbns[i];
        cluster_map[isbn] = init[i];
    }

    int nchanged = nedges;
    int iter = 0;
    while (nchanged > 0) {
        nchanged = 0;
        iter += 1;
        std::cerr <<": starting iteration " <<iter <<" at " <<std::endl;
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

        std::cerr <<"iteration " <<iter <<" changed " <<nchanged <<" memberships "<<std::endl;
    }

    IntegerVector out(nisbns);
    for (int i = 0; i < nisbns; i++) {
        out[i] = cluster_map[isbns[i]];
    }
    return out;
}
