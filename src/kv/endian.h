#pragma once
#include <cstdint>

inline void put_u32_le(uint32_t v, unsigned char* p){
    p[0]=uint8_t(v); p[1]=uint8_t(v>>8); p[2]=uint8_t(v>>16); p[3]=uint8_t(v>>24);
}
inline void put_u64_le(uint64_t v, unsigned char* p){
    for (int i=0;i<8;++i) p[i]=uint8_t(v>>(8*i));
}
inline uint32_t get_u32_le(const unsigned char* p){
    return (uint32_t)p[0] | ((uint32_t)p[1]<<8) | ((uint32_t)p[2]<<16) | ((uint32_t)p[3]<<24);
}
inline uint64_t get_u64_le(const unsigned char* p){
    uint64_t v=0; for (int i=0;i<8;++i) v |= (uint64_t)p[i]<<(8*i); return v;
}
