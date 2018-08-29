#include "ps/ps.h"
#include <unistd.h>
using namespace ps;
using namespace std;

#define HAVE_CAFFE

// SSP connection test
// template <typename Val>
// struct KVServerSSPHandle {
//using WorkerId = uint64_t;
using Staleness = uint64_t;
using Callback = std::function<void()>;

template<class Val>
class KVServerSSPHandle {
public:
	/**
	 *req_meta: the meta info about this request, including cmd push sender timestamp customer_id
	 req_data: the data, key, value, value lens
	 server: the pointer of the current PS
	 */
	void operator()(const KVMeta& req_meta, const KVPairs<Val>& req_data,
			KVServer<Val>* server) {
		size_t n = req_data.keys.size();
		KVPairs<Val> res;
		if (req_meta.push) {
			CHECK_EQ(n, req_data.vals.size());
		} else {
			res.keys = req_data.keys;
			res.vals.resize(n);
		}
		int current_iter = req_meta.staleness;
		Key skey = req_data.keys[0];
		if (req_meta.push) {
			for (size_t i = 0; i < n; ++i) {
				Key key = req_data.keys[i];
				store[key] += req_data.vals[i];
			}
			workercount[current_iter] += 1; // For this iteration, add one
			while (workercount[ticks[skey]] == NumWorkers()) { // For a given key, if the staleness has been passed number of workers, add one
				//trigger a cb of pull
				auto& cbs = callbacks_[ticks[skey]];
				for (const auto& cb : cbs) {
					cb();
				}
				ticks[skey] += 1;
			}
		} else {
			/*
			 * SSP condition
			 * the slowest one + stale <= current_iter
			 * we can not pull data until
			 */
			if (ticks[skey] + server->stale <= current_iter) { // Wait
				//wait for the slow workers catch up
				callbacks_[ticks[skey]].push_back(
						[this, req_meta, req_data, res, server]() mutable {
							size_t n = req_data.keys.size();
							for (size_t i = 0; i < n; ++i) {
								Key key = req_data.keys[i];
								res.vals[i] = store[key];
							}
							server->Response(req_meta, res);
						});
				return;
			}
			for (size_t i = 0; i < n; ++i) {
				Key key = req_data.keys[i];
				res.vals[i] = store[key];
			}
		}
		server->Response(req_meta, res);
	}
public:
	unordered_map<Key, Val> store;
	unordered_map<Key, Staleness> ticks;
	unordered_map<Staleness, int> workercount;
	unordered_map<Staleness, std::vector<Callback>> callbacks_;

};

template<class Val>
class KVServerSSPHandle_Caffe {
public:
	/**
	 *req_meta: the meta info about this request, including cmd push sender timestamp customer_id
	 req_data: the data, key, value, value lens
	 server: the pointer of the current PS
	 */
	void operator()(const KVMeta& req_meta, const KVPairs<Val>& req_data,
			KVServer<Val>* server) {
		size_t n = req_data.keys.size();
		KVPairs<Val> res;
		if (req_meta.push) {
			CHECK_EQ(n, req_data.lens.size());// matching key array with lens array. key->value->lens
			size_t lens = 0;
			for (size_t i = 0; i < n; ++i) {
				lens += req_data.lens[i];
			}
			CHECK_EQ(lens, req_data.vals.size());
		} else {
			res.keys = req_data.keys;
			res.lens.resize(n);
		}
		int current_iter = req_meta.staleness;
		Key skey = req_data.keys[0];
		if (req_meta.push) {
			size_t tolal_lens = 0;
			for (size_t i = 0; i < n; ++i) {
				size_t lens = req_data.lens[i];
				// checking values with lens CHECK_EQ(n, req_data.lens.size());
				Key key = req_data.keys[i];
				if (!store[key].size()) {
					for (size_t j = 0; j < lens; j++) {
						store[key].push_back(req_data.vals[tolal_lens + j]); // init
					}
				} else {
					for (size_t j = 0; j < lens; j++) {
						store[key][j] += req_data.vals[tolal_lens + j]; //further add
					}
				}
				tolal_lens += lens;
			}
			workercount[current_iter] += 1; // For this iteration, add one
			while (workercount[ticks[skey]] == NumWorkers()) { // For a given key, if the staleness has been passed number of workers, add one
				//trigger a cb of pull
				auto& cbs = callbacks_[ticks[skey]];
				for (const auto& cb : cbs) {
					cb();
				}
				ticks[skey] += 1;
			}
		} else {
			/*
			 * SSP condition
			 * the slowest one + stale <= current_iter
			 * we can not pull data until
			 */
			if (ticks[skey] + server->stale <= current_iter) { // Wait
				//wait for the slow workers catch up
				callbacks_[ticks[skey]].push_back(
						[this, req_meta, req_data, res, server]() mutable {
							size_t n = req_data.keys.size();
							for (size_t i = 0; i < n; ++i) {
								Key key = req_data.keys[i];
								size_t lens = store[key].size();
								res.lens[i] = lens;
								for(size_t j = 0; j < lens; j++) {
									res.vals.push_back(store[key][j]);
								}
							}
							server->Response(req_meta, res);
						});
				return;
			}
			for (size_t i = 0; i < n; ++i) {
				Key key = req_data.keys[i];
				size_t lens = store[key].size();
				res.lens[i] = lens;
				for (size_t j = 0; j < lens; j++) {
					res.vals.push_back(store[key][j]);
				}
			}
		}
		server->Response(req_meta, res);
	}
public:
	unordered_map<Key, std::vector<Val>> store;
	unordered_map<Key, Staleness> ticks;
	unordered_map<Staleness, int> workercount;
	unordered_map<Staleness, std::vector<Callback>> callbacks_;

};

void StartServer() {
	if (!IsServer()) {
		return;
	}
	cout << "num of workers[" << NumWorkers() << "]" << endl;
	cout << "num of servers[" << NumServers() << "]" << endl;
	auto server = new KVServer<float>(0);
	server->stale = 0;
#ifdef HAVE_CAFFE
	server->set_request_handle(KVServerSSPHandle_Caffe<float>());
#else
	server->set_request_handle(KVServerSSPHandle<float>());
#endif
	RegisterExitCallback([server]() {delete server;});
}

void RunWorker() {
	if (!IsWorker())
		return;
	KVWorker<float> kv(0, 0);

	// init
	int num = 10000;
	vector<Key> keys(num);
	vector<float> vals(num);

	int rank = MyRank();
	srand(rank + 7);
	for (int i = 0; i < num; ++i) {
		keys[i] = kMaxKey / num * i; // kMaxKey / num * i + rank, two worker should update the same key
		vals[i] = i; //(rand() % 1000);
	}

	// push
	int repeat = 50;
	vector<int> ts;
	vector<float> rets;
	for (int i = 0; i < repeat; ++i) {
		cout << "enter iteration: " << i << endl;
		//SSP
		ts.push_back(kv.sPush(keys, vals, i));
		kv.Wait(kv.sPull(keys, &rets, i));
	}
	//sync the rest
	for (int t : ts)
		kv.Wait(t);

	// pull
	kv.Wait(kv.Pull(keys, &rets));

	float res = 0;
	for (int i = 0; i < num; ++i) {
		res += fabs(rets[i] - vals[i] * repeat * NumWorkers());
	}
	CHECK_LT(res / repeat, 1e-5);
	LL<< "error: " << res / repeat;
}

void RunWorker_Caffe() {
	if (!IsWorker())
		return;
	KVWorker<float> kv(0, 0);

	// init
	int num = 10000;
	vector<Key> keys(1, 1);
	vector<float> vals(num);
	vector<int> len(1, num);

	int rank = MyRank();
	srand(rank + 7);
	for (int i = 0; i < num; ++i) {
		//vals.push_back(i);
		vals[i] = i;
	}

	// push
	int repeat = 50;
	vector<int> ts;
	vector<float> rets;
	std::vector<int> ret_len;
	for (int i = 0; i < repeat; ++i) {
		cout << "enter iteration: " << i << endl;
		//SSP
		ts.push_back(kv.sPush(keys, vals, i, len));
		//cout << "finish push, enter pull " << i << endl;
		kv.Wait(kv.sPull(keys, &rets, i, &ret_len));
	}
	//sync the rest
	for (int t : ts)
		kv.Wait(t);

	// pull
	kv.Wait(kv.Pull(keys, &rets, &ret_len));

	float res = 0;
	for (int i = 0; i < num; ++i) {
		res += fabs(rets[i] - vals[i] * repeat * NumWorkers());
	}
	CHECK_LT(res / repeat, 1e-5);
	LL<< "error: " << res / repeat;
}


int main(int argc, char *argv[]) {
	// start system
	Start(0);
	// setup server nodes
	StartServer();
	// run worker nodes
	//RunWorker();
#ifdef HAVE_CAFFE
	RunWorker_Caffe();
#else
	RunWorker();
#endif
	// stop system
	Finalize(0, true);
	return 0;
}
