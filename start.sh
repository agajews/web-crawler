#!/bin/bash

mkdir $CRAWLER_FS/url-block-dir
export URL_BLOCK_DIR=$CRAWLER_FS/url-block-dir

mkdir $CRAWLER_FS/claimed-url-block-dir
export URL_CLAIMED_BLOCK_DIR=$CRAWLER_FS/claimed-url-block-dir

mkdir $CRAWLER_FS/url-metadata
export META_DIR=$CRAWLER_FS/url-metadata

mkdir $CRAWLER_FS/url-indexes
export INDEX_DIR=$CRAWLER_FS/url-indexes

export SEEN_URL_PATH=$CRAWLER_FS/seen-urls.sled

web-crawler
