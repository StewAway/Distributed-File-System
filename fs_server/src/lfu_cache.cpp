#include "fs_server/lfu_cache.hpp"
#include <iostream>
#include <algorithm>

namespace fs_server {

LFUCache::LFUCache(size_t max_cache_size_mb)
    : max_size_(max_cache_size_mb * 1024 * 1024) {
    std::cout << "LFUCache: Initialized with max size: " << max_cache_size_mb << " MB" << std::endl;
}

LFUCache::~LFUCache() {
    Clear();
}

bool LFUCache::Get(uint64_t block_uuid, uint32_t offset, uint32_t length,
                   std::string& out_data) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    auto it = cache_map_.find(block_uuid);
    if (it == cache_map_.end()) {
        stats_.misses++;
        std::cout << "LFUCache: Cache MISS for block " << block_uuid << std::endl;
        return false;  // Cache miss
    }

    // Cache hit - update frequency
    stats_.hits++;
    const std::string& cached_data = it->second.first;
    uint64_t old_frequency = it->second.second;
    
    // Validate offset
    if (offset > cached_data.length()) {
        out_data.clear();
        return true;
    }
    
    // Calculate actual read length
    uint32_t actual_length = length;
    if (length == 0) {
        actual_length = cached_data.length() - offset;
    } else {
        uint32_t max_available = cached_data.length() - offset;
        actual_length = std::min(length, max_available);
    }
    
    // Extract requested portion
    out_data = cached_data.substr(offset, actual_length);
    
    // Update frequency
    UpdateFrequency(block_uuid);
    
    std::cout << "LFUCache: Cache HIT for block " << block_uuid 
              << " (freq=" << it->second.second << ", " << out_data.length() << " bytes)" << std::endl;
    
    return true;  // Cache hit
}

bool LFUCache::Put(uint64_t block_uuid, const std::string& data) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // If block already exists, remove it first
    if (cache_map_.find(block_uuid) != cache_map_.end()) {
        Remove(block_uuid);
    }
    
    // Check if we need to evict
    while (cache_map_.size() > 0 && 
           (cache_map_.size() * 100000 + data.length() > max_size_)) {
        EvictLFU();
    }
    
    // Add to cache with frequency 1
    cache_map_[block_uuid] = {data, 1};
    frequency_map_[1].push_back(block_uuid);
    min_frequency_ = 1;
    
    std::cout << "LFUCache: Cached block " << block_uuid 
              << " (" << data.length() << " bytes)" << std::endl;
    
    return true;
}

bool LFUCache::Remove(uint64_t block_uuid) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    auto it = cache_map_.find(block_uuid);
    if (it == cache_map_.end()) {
        return false;
    }
    
    uint64_t frequency = it->second.second;
    
    // Remove from frequency list
    auto& freq_list = frequency_map_[frequency];
    auto freq_it = std::find(freq_list.begin(), freq_list.end(), block_uuid);
    if (freq_it != freq_list.end()) {
        freq_list.erase(freq_it);
    }
    
    // Remove from map
    cache_map_.erase(it);
    
    return true;
}

bool LFUCache::Contains(uint64_t block_uuid) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    return cache_map_.find(block_uuid) != cache_map_.end();
}

void LFUCache::Clear() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    cache_map_.clear();
    frequency_map_.clear();
    min_frequency_ = 1;
}

void LFUCache::EvictLFU() {
    // Evict the least frequently used block
    // If min_frequency doesn't exist, find the next one
    while (frequency_map_.find(min_frequency_) == frequency_map_.end() && 
           min_frequency_ < 1000000) {
        min_frequency_++;
    }
    
    if (frequency_map_.find(min_frequency_) != frequency_map_.end()) {
        auto& freq_list = frequency_map_[min_frequency_];
        if (!freq_list.empty()) {
            uint64_t lfu_uuid = freq_list.front();  // Evict earliest inserted
            freq_list.pop_front();
            cache_map_.erase(lfu_uuid);
            stats_.evictions++;
            std::cout << "LFUCache: Evicted block " << lfu_uuid 
                      << " (frequency=" << min_frequency_ << ")" << std::endl;
        }
    }
}

void LFUCache::UpdateFrequency(uint64_t block_uuid) {
    auto it = cache_map_.find(block_uuid);
    if (it == cache_map_.end()) return;
    
    uint64_t old_freq = it->second.second;
    uint64_t new_freq = old_freq + 1;
    
    // Remove from old frequency list
    auto& old_freq_list = frequency_map_[old_freq];
    auto freq_it = std::find(old_freq_list.begin(), old_freq_list.end(), block_uuid);
    if (freq_it != old_freq_list.end()) {
        old_freq_list.erase(freq_it);
    }
    
    // Add to new frequency list
    frequency_map_[new_freq].push_back(block_uuid);
    
    // Update cache map
    it->second.second = new_freq;
    
    // Update min frequency if old frequency list is now empty
    if (old_freq_list.empty() && old_freq == min_frequency_) {
        min_frequency_ = new_freq;
    }
}

PageCachePolicy::CacheStats LFUCache::GetStats() const {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // Calculate current size
    size_t current_size = 0;
    for (const auto& pair : cache_map_) {
        current_size += pair.second.first.length();
    }
    
    return {
        stats_.hits,
        stats_.misses,
        stats_.evictions,
        current_size,
        max_size_,
        "LFU"
    };
}

void LFUCache::ResetStats() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    stats_.hits = 0;
    stats_.misses = 0;
    stats_.evictions = 0;
}

}  // namespace fs_server
