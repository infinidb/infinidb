REM You need to uncomment one of these sections below at a time
REM Then you need to copy the libs from stage to \InfiniDB
REM
REM build just 64-bit release libs
REM b2 --clean
b2 --with-date_time --with-filesystem --with-regex --with-system --with-thread --with-chrono address-model=64 variant=release stage

REM build just 64-bit debug libs
REM bjam --clean
REM bjam --with-date_time --with-filesystem --with-regex --with-system --with-thread address-model=64 variant=debug stage

