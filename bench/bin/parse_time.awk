#!/usr/bin/awk -f
/Elapsed \(wall clock\)/ {
    timestr = $0
    sub(/.*\): /, "", timestr)
    n = split(timestr, t, ":")
    if (n == 2) {
        gsub(/[^0-9.]/, "", t[2])
        wall = t[1]*60 + t[2]
    } else if (n == 3) {
        gsub(/[^0-9.]/, "", t[3])
        wall = t[1]*3600 + t[2]*60 + t[3]
    } else {
        # "Xm Ys" or "Xh Ym Zs" format
        h = 0; m_val = 0; s = 0
        if (match(timestr, /[0-9.]+h/)) h     = substr(timestr, RSTART, RLENGTH-1) + 0
        if (match(timestr, /[0-9.]+m/)) m_val = substr(timestr, RSTART, RLENGTH-1) + 0
        if (match(timestr, /[0-9.]+s/)) s     = substr(timestr, RSTART, RLENGTH-1) + 0
        wall = h*3600 + m_val*60 + s
    }
}
/Maximum resident set size/ { rss = int($NF / 1024) }
/Percent of CPU/             { gsub(/%/, "", $NF); cpu = $NF }
/Exit status/                { exit_code = $NF }
END {
    print "wall_seconds\tpeak_rss_mb\tcpu_percent\texit_code"
    printf "%.3f\t%s\t%s\t%s\n", wall, rss, cpu, exit_code
}
