# cron
A fast non-allocating ron parser in ragel and golang.


# features
- [x] standard five-position cron
- [x] six-position cron with seconds
- [x] seven-position cron with seconds and years
- [x] case-insensitive days of the week
- [x] case-insensitive month names
- [x] [Quartz](http://www.quartz-scheduler.org) compatible ranges e.g.: `4/10`, `SUN-THURS/2`
- [ ] timezone handling (other than UTC)
- [ ] Quartz compatible # handling, i.e. `5#3` meaning the third friday of the month
- [ ] Quartz compatible L handling, i.e. `5L` in the day of week field meaning the last friday of a month, or `3L` in the day of month field meaning third to last day of the month

# performance
On a 3.1 Ghz Core i7 MacBook Pro (Retina, 15-inch, Mid 2017):

| | | | |
|-|-|-|-|
| `BenchmarkParse/0_*_*_*_*_*_*-8` | 20000000 |  63.5 ns/op |  0 B/op | 0 allocs/op |
| `BenchmarkParse/1-6/2_*_*_*_Feb-Oct/3_*_*-8` | 10000000 |  118  ns/op |  0 B/op | 0 allocs/op |
| `BenchmarkParse/1-6/2_*_*_*_Feb-Oct/3_*_2020/4-8` |  1000000 |  146 ns/op |  0 B/op | 0 allocs/op |

# TODO
- Once we are done adding all of the Quartz cron features, to copy and pass all of Quartz's parsing tests.
