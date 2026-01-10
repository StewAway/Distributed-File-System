#include "fs_server/lfu_cache.hpp"
#include <iostream>

namespace fs_server {

LFUCache::LFUCache(size_t cache_size)
    : capacity_(cache_size), size_(0), min_freq_(1) {
    std::cout << "LFUCache: Initialized with max pages: " << cache_size << std::endl;
}

LFUCache::~LFUCache() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // Clean up all nodes
    for (auto& pair : cache_map_) {
        delete pair.second;
    }
    cache_map_.clear();
    
    // Clean up frequency lists
    for (auto& pair : freq_map_) {
        delete pair.second;
    }
    freq_map_.clear();
}

FrequencyList* LFUCache::getOrCreateFreqList(uint64_t freq) {
    if (freq_map_.find(freq) == freq_map_.end()) {
        freq_map_[freq] = new FrequencyList();
    }
    return freq_map_[freq];
}

bool LFUCache::Get(uint64_t block_uuid, std::string& out_data) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    if (cache_map_.find(block_uuid) != cache_map_.end()) {
        LFUNode* node = cache_map_[block_uuid];
        
        // 1) Remove node from current frequency list
        FrequencyList* old_freq_list = freq_map_[node->freq];
        old_freq_list->remove(node);
        
        // 2) Update min_freq if needed
        if (min_freq_ == node->freq && old_freq_list->isEmpty()) {
            min_freq_++;
        }
        
        // 3) Increase frequency and add to new frequency list
        node->freq++;
        FrequencyList* new_freq_list = getOrCreateFreqList(node->freq);
        new_freq_list->add(node);
        
        // 4) Return data
        out_data = node->page.data;
        stats_.hits++;
        return true;
    }
    
    stats_.misses++;
    return false;
}

bool LFUCache::Put(uint64_t block_uuid, const std::string& data, bool dirty) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    if (capacity_ == 0) return false;
    
    if (cache_map_.find(block_uuid) != cache_map_.end()) {
        // Block already exists - update it
        LFUNode* node = cache_map_[block_uuid];
        
        // 1) Remove from current frequency list
        FrequencyList* old_freq_list = freq_map_[node->freq];
        old_freq_list->remove(node);
        
        // 2) Update data and dirty flag
        node->page.data = data;
        node->page.dirty = dirty;
        
        // 3) Update min_freq if this was the only node at min_freq
        if (min_freq_ == node->freq && old_freq_list->isEmpty()) {
            min_freq_++;
        }
        
        // 4) Increase frequency and add to new frequency list
        node->freq++;
        FrequencyList* new_freq_list = getOrCreateFreqList(node->freq);
        new_freq_list->add(node);
        
        return true;
    } else {
        // New block - check if we need to evict
        if (size_ >= capacity_) {
            EvictLFU();
        }
        
        // Create new node with freq = 1
        LFUNode* node = new LFUNode(block_uuid, data, dirty);
        
        // Add to frequency 1 list
        FrequencyList* freq_list = getOrCreateFreqList(1);
        freq_list->add(node);
        
        // Add to cache map
        cache_map_[block_uuid] = node;
        size_++;
        min_freq_ = 1;  // New nodes always have freq = 1
        
        return true;
    }
}

void LFUCache::EvictLFU() {
    // Find the LFU node (tail of min_freq list - LRU among same frequency)
    if (freq_map_.find(min_freq_) == freq_map_.end() || freq_map_[min_freq_]->isEmpty()) {
        std::cerr << "LFUCache: EvictLFU called but no node to evict" << std::endl;
        return;
    }
    
    FrequencyList* min_freq_list = freq_map_[min_freq_];
    LFUNode* lfu_node = min_freq_list->getTail();
    
    if (lfu_node == nullptr) {
        std::cerr << "LFUCache: EvictLFU - getTail returned nullptr" << std::endl;
        return;
    }
    
    // Write-back: flush dirty page before eviction
    if (lfu_node->page.dirty && eviction_callback_) {
        std::cout << "LFUCache: Flushing dirty block " << lfu_node->block_uuid 
                  << " before eviction" << std::endl;
        eviction_callback_(lfu_node->block_uuid, lfu_node->page.data);
    }
    
    // Remove from frequency list
    min_freq_list->remove(lfu_node);
    
    // Remove from cache map
    cache_map_.erase(lfu_node->block_uuid);
    
    // Delete node
    delete lfu_node;
    
    size_--;
    stats_.evictions++;
}

bool LFUCache::Remove(uint64_t block_uuid) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    if (cache_map_.find(block_uuid) != cache_map_.end()) {
        LFUNode* node = cache_map_[block_uuid];
        
        // Remove from frequency list
        if (freq_map_.find(node->freq) != freq_map_.end()) {
            freq_map_[node->freq]->remove(node);
        }
        
        // Remove from cache map
        cache_map_.erase(block_uuid);
        
        // Delete node
        delete node;
        size_--;
        
        return true;
    }
    
    return false;
}

bool LFUCache::Contains(uint64_t block_uuid) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    return cache_map_.find(block_uuid) != cache_map_.end();
}

void LFUCache::Clear() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // Delete all nodes
    for (auto& pair : cache_map_) {
        delete pair.second;
    }
    cache_map_.clear();
    
    // Clear frequency lists (but keep the FrequencyList objects)
    for (auto& pair : freq_map_) {
        delete pair.second;
    }
    freq_map_.clear();
    
    size_ = 0;
    min_freq_ = 1;
}

PageCachePolicy::CacheStats LFUCache::GetStats() const {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    return {
        stats_.hits,
        stats_.misses,
        stats_.evictions,
        "LFU"
    };
}

void LFUCache::ResetStats() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    stats_.hits = 0;
    stats_.misses = 0;
    stats_.evictions = 0;
}

void LFUCache::SetEvictionCallback(EvictionCallback callback) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    eviction_callback_ = std::move(callback);
    std::cout << "LFUCache: Eviction callback registered" << std::endl;
}

void LFUCache::FlushAll() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    if (!eviction_callback_) {
        std::cout << "LFUCache: FlushAll called but no eviction callback set" << std::endl;
        return;
    }
    
    int flushed_count = 0;
    for (auto& pair : cache_map_) {
        LFUNode* node = pair.second;
        if (node->page.dirty) {
            std::cout << "LFUCache: FlushAll - flushing dirty block " 
                      << node->block_uuid << std::endl;
            eviction_callback_(node->block_uuid, node->page.data);
            node->page.dirty = false;
            flushed_count++;
        }
    }
    
    std::cout << "LFUCache: FlushAll completed, flushed " << flushed_count 
              << " dirty pages" << std::endl;
}

}  // namespace fs_server
