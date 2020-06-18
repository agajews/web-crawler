#!/bin/bash

WEB_CRAWLER=./target/release/web-crawler

mkdir $CRAWLER_FS/url-block-dir
export URL_BLOCK_DIR=$CRAWLER_FS/url-block-dir

mkdir $CRAWLER_FS/claimed-url-block-dir
export URL_CLAIMED_BLOCK_DIR=$CRAWLER_FS/claimed-url-block-dir

mkdir $CRAWLER_FS/url-metadata
export META_DIR=$CRAWLER_FS/url-metadata

mkdir $CRAWLER_FS/url-indexes
export INDEX_DIR=$CRAWLER_FS/url-indexes

export SEEN_URL_PATH=$CRAWLER_FS/seen-urls.sled

for i in {1..7}; do $WEB_CRAWLER & done
$WEB_CRAWLER
