#include "crc32.h"
static uint32_t table_[256];
static bool inited=false;

static void init(){
    for (uint32_t i=0;i<256;i++){
        uint32_t c=i;
        for (int j=0;j<8;j++) c = (c&1)?(0xEDB88320u ^ (c>>1)):(c>>1);
        table_[i]=c;
    }
    inited=true;
}
uint32_t crc32(const void* data, size_t len, uint32_t seed){
    if(!inited) init();
    auto* p = static_cast<const unsigned char*>(data);
    uint32_t c = seed;
    for (size_t i=0;i<len;++i) c = table_[(c ^ p[i]) & 0xFF] ^ (c >> 8);
    return c ^ 0xFFFFFFFFu;
}
