
#include <set>
#include <map>
#include <thread>
#include <ctime>
#include <list>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include "tsmap.h"

#include <iostream>


using namespace std::chrono_literals;



#define LOCK_TIEMOUT 10s


#define CHECK_LOCK\
    std::thread::id tid = std::this_thread::get_id();\
    {\
        auto itLock=lockItems_->find(k);\
        while(itLock!=lockItems_->end() && itLock->second.second!=tid)\
        {\
            {\
                std::unique_lock<std::mutex> lk(cv_m_);\
                if(!cv_.wait_for(lk,LOCK_TIEMOUT, []{return true;}))\
                { throw std::runtime_error("Resource Busy");}\
            }\
            itLock=lockItems_->find(k);\
        }\
    }

#define CHECK_READ_LOCK\
    std::thread::id tid = std::this_thread::get_id();\
    {\
        auto itLock=lockItems_->find(k);\
        while(itLock!=lockItems_->end() && itLock->second.second!=tid && itLock->second.first==READ_WRITE)\
        {\
            {\
                std::unique_lock<std::mutex> lk(cv_m_);\
                cv_.wait(lk, []{return true;});\
            }\
            itLock=lockItems_->find(k);\
        }\
    }


template<typename Key, typename Value>
class Cache {
public:

 
    typedef typename std::pair<Key, Value> key_value_pair_t;
    typedef typename std::list<key_value_pair_t>::iterator list_iterator_t;

    enum Mode {FIFO, LRU};
    enum LockType {READONLY, READ_WRITE};

    Key Put(std::shared_ptr<Value> v)
    {   
        const std::unique_lock<std::shared_mutex> lock(protect_); 
        auto id = PutItem(v);
        PutCache(id,v);  
        return id;
    } 
    
    std::shared_ptr<Value>  Get(const Key& k)
    {
        CHECK_READ_LOCK   
        std::unique_lock lock(protect_);
        std::shared_ptr<Value> val=std::make_shared<Value>();
        auto it = cache_->find(k);
		if (it == cache_->end()) {            
			if(GetItem(k,val))
            {
                PutCache(k,val);
                lock.unlock();
                return Get(k);
            }
            else
            {
                return nullptr;
            }
		} else {
            auto ret= Clone(it->second->second);
            if(cacheMode_==LRU)
            {                
			    cacheItems_->splice(cacheItems_->begin(), *cacheItems_, it->second);
               /* RemoveIndexMap(ret,it->second);        
                cache_->erase(it);
                cache_->emplace(k,cacheItems_->begin());   
                AddIndexMap(ret,cacheItems_->begin()); */
            }
			return ret;
		}        
    }

    bool Check(const Key& k)
    {
        const std::shared_lock<std::shared_mutex> lock(protect_);
        if (cache_->find(k) == cache_->end()) {
			std::shared_ptr<Value>  val=std::make_shared<Value>();
			if(!GetItem(k,val))
            {
                return false;
            }
		} 

		return true;
    }
    void Change(const Key& k, std::shared_ptr<Value> v)
    {
        CHECK_LOCK        
        std::unique_lock lock(protect_);   
        auto it = cache_->find(k);
		if (it == cache_->end()) {
            std::shared_ptr<Value> val=std::make_shared<Value>();
            if(GetItem(k,val))
            {
                PutCache(k,val);
                lock.unlock();
                return Change(k,v);
            }
            else
            {
                throw std::runtime_error("record not found");
            }            
        }

        UpdateItem(k,v);
        cacheItems_->erase(it->second);
        cacheItems_->push_front(key_value_pair_t(k, *Clone(*v.get())));   
        RemoveIndexMap(v,it->second);        
        cache_->erase(it);
        cache_->emplace(k,cacheItems_->begin());   
        AddIndexMap(v,cacheItems_->begin()); 
    }

    void Erase(const Key& k)
    {        
        CHECK_LOCK
        const std::unique_lock<std::shared_mutex> lock(protect_);
        EraseItem(k);
        EraseCache(k);        
    }    
 

    void Clear()
    {
        const std::unique_lock<std::shared_mutex> lock(protect_);
        cacheItems_->clear();
        cache_->clear();
        ClearIndexMap();
    }

    void Lock(const Key& k,LockType lockType)
    {
        CHECK_LOCK
        std::unique_lock<std::mutex> lk(cv_m_);
        auto it=lockItems_->find(k);
        if(it==lockItems_->end())
        {
            lockItems_->emplace(k,std::make_pair(lockType,tid));
        }
        else if(lockType==READ_WRITE)
            it->second=std::make_pair(lockType,tid);
    }

    void Free(const Key& k)
    {
        CHECK_LOCK
        std::unique_lock<std::mutex> lk(cv_m_);
        auto it = lockItems_->find(k);
        if(it!=lockItems_->end() && it->second.second==tid)
        {
            lockItems_->erase(it);
        }        
        cv_.notify_all();
    }

    size_t Size() const
    {
        const std::shared_lock<std::shared_mutex> lock(protect_);
        return cache_!=nullptr?cache_->size():0;
    }
    
    void Init(size_t size,Mode mode=LRU)
    {
        const std::unique_lock<std::shared_mutex> lock(protect_);
        maxCacheSize_ = size;
        cacheMode_=mode;
    }

    Cache()
    {
        cache_=std::make_shared<std::map<Key,list_iterator_t>>();
        cacheItems_ = std::make_shared<std::list<key_value_pair_t>>();
        lockItems_ = std::make_shared<tsmap<Key,std::pair<LockType,std::thread::id>>>();
        Init(SIZE_MAX,LRU);
    }
    ~Cache()
    {

    }
    
    virtual void Load()=0;
protected:
    virtual bool GetItem(const Key& k,std::shared_ptr<Value> v)=0;  
    virtual Key PutItem(std::shared_ptr<Value> v)=0;  
    virtual void UpdateItem(const Key& k, std::shared_ptr<Value> v)=0;  
    virtual void EraseItem(const Key& k)=0;
    virtual std::shared_ptr<Value> Clone(Value& v)=0; 
    virtual void AddIndexMap(std::shared_ptr<Value> v,list_iterator_t it) =0;
    virtual void RemoveIndexMap(std::shared_ptr<Value> v,list_iterator_t it) =0;
    virtual void ClearIndexMap() =0;

    inline void EraseCache(const Key& k)
    {        
        auto it = cache_->find(k);
		if (it != cache_->end()) {
            RemoveIndexMap(Clone(it->second->second),it->second);   
            cacheItems_->erase(it->second);
            cache_->erase(it);             
		}             
    }  

    inline list_iterator_t PutCache(const Key& k, std::shared_ptr<Value> v,bool updateFlg=false)
    {
        auto it=cache_->find(k);
        bool isExist = false;
        auto ret= it->second;
        if(it!=cache_->end())
        {
            isExist = true;
            if(cacheMode_==LRU || updateFlg)
            {
                if(!updateFlg)
                {
                    v = Clone(it->second->second);
                }
                RemoveIndexMap(Clone(it->second->second),it->second);
                cacheItems_->erase(it->second);
                cache_->erase(it); 
            }
        }

        if(!isExist || updateFlg || cacheMode_==LRU)
        {
            cacheItems_->push_front(key_value_pair_t(k, *Clone(*v.get())));
            ret = cacheItems_->begin();    
            cache_->emplace(k,ret);
            AddIndexMap(v,ret);
            
        }

        if(cache_->size()>=maxCacheSize_)
        {
            auto last = cacheItems_->end();
            last--;
            while (lockItems_->find(last->first)!=lockItems_->end() && last!=cacheItems_->begin()) last--;
            if(last!=cacheItems_->begin())
            {
                RemoveIndexMap(Clone(last->second),last);
                cache_->erase(last->first);
                cacheItems_->erase(last);                
            }
        }    
        
        return ret;
    }

    std::shared_ptr<tsmap<Key,std::pair<LockType,std::thread::id>>> lockItems_;
    std::shared_ptr<std::list<key_value_pair_t>> cacheItems_;
    std::shared_ptr<std::map<Key,list_iterator_t>> cache_;
    Mode cacheMode_;
    size_t maxCacheSize_;
    std::mutex put_mutex_;
    std::condition_variable cv_;
    std::mutex cv_m_;
    mutable std::shared_mutex protect_;
};
