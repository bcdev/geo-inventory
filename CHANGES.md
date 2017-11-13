# 0.6.0 (13.11.2017)
 * constrain can have multiple date ranges
 * change constrain builder API
 * support reading from an (not seekable) InputStream
# 0.5.2 (24.10.2017)
 * products without a time are return as a result for all constraints 
 * in DB dumps write a not exiting date as "null"
 * ensure that either start and date are present or none of them   
# 0.5.1 (18.10.2017)
 * fix updates from empty scan files
 * reduce logging
# 0.5 (08.09.2017)
 * big rewrite
 * binary index in a single file
 * save update procedure, to prevent corruption of old index while updating
 * ability to read binary index plus existing scan files
# 0.1 (02.12.2016)
 * first release
 