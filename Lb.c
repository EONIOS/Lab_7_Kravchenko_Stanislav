# include <iostream>
# include <vector>
# include <queue>
# include <thread>
# include <mutex>
# include <condition_variable>
# include <semaphore>
# include <random>
# include <chrono>
# include <memory>

std::mutex cout_mtx; 

class AbstractBuffer
{
	public:
    int capacity;
	int producerCount;

	AbstractBuffer(int cap, int prodCount) : capacity(cap), producerCount(prodCount) { }
	virtual ~AbstractBuffer() = default;

    virtual void Produce(int producerId, int value) = 0;
    virtual int Consume(int producerId) = 0;
};

class Producer
{
	private:
    int id;
	int itemsCount;
	AbstractBuffer& buffer;

public:
    Producer(int id, int itemsCount, AbstractBuffer& buf)
		: id(id), itemsCount(itemsCount), buffer(buf) { }

	void Run()
	{
		{
			std::lock_guard < std::mutex > lock (cout_mtx) ;
			std::cout << "[Виробник " << id << "] STARTED: всього елементів = " << itemsCount << "\n";
		}

		std::random_device rd;
		std::mt19937 gen(rd() +id); 
		std::uniform_int_distribution<> dis(1, 100);
		std::uniform_int_distribution<> sleepDis(50, 150);

		for (int i = 0; i < itemsCount; ++i)
		{
			int value = dis(gen);
			std::this_thread::sleep_for(std::chrono::milliseconds(sleepDis(gen)));
			buffer.Produce(id, value);

			std::lock_guard < std::mutex > lock (cout_mtx) ;
			std::cout << "[Виробник " << id << "] Згенерував значення " << value << " (" << (i + 1) << "/" << itemsCount << ")\n";
		}

		std::lock_guard < std::mutex > lock (cout_mtx) ;
		std::cout << "[Виробник " << id << "] FINISHED.\n";
	}
};

class Consumer
{
	private:
    int producerCount;
	std::vector<int> itemsPerProducer;
	AbstractBuffer& buffer;

public:
    Consumer(int prodCount, const std::vector<int>& itemsCounts, AbstractBuffer& buf)
        : producerCount(prodCount), itemsPerProducer(itemsCounts), buffer(buf) { }

	void Run()
	{
		{
			std::lock_guard < std::mutex > lock (cout_mtx) ;
			std::cout << "[Споживач] STARTED.\n";
		}

		for (int i = 1; i <= producerCount; ++i)
		{
			int totalToRead = itemsPerProducer[i - 1];
			{
				std::lock_guard < std::mutex > lock (cout_mtx) ;
				std::cout << "[Споживач] Починаю зчитування від Виробника " << i << " (всього: " << totalToRead << ")...\n";
			}

			for (int j = 0; j < totalToRead; ++j)
			{
				int val = buffer.Consume(i);
				{
					std::lock_guard < std::mutex > lock (cout_mtx) ;
					std::cout << "==> [Споживач] Отримав значення " << val << " від Виробника " << i << " (" << (j + 1) << "/" << totalToRead << ")\n";
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(50));
			}
		}

		std::lock_guard < std::mutex > lock (cout_mtx) ;
		std::cout << "[Споживач] FINISHED: усі дані успішно зчитано!\n";
	}
};

class BufferSemaphore : public AbstractBuffer {
private:
    std::mutex mtx;
std::counting_semaphore<> slotsFree;
std::vector<std::unique_ptr<std::counting_semaphore<>>> itemsAvailable;
std::vector<std::queue<int>> queues;

public:
    BufferSemaphore(int cap, int prodCount)
        : AbstractBuffer(cap, prodCount), slotsFree(cap) {
	queues.resize(prodCount + 1);
	for (int i = 0; i <= prodCount; ++i)
	{
		itemsAvailable.push_back(std::make_unique<std::counting_semaphore<>>(0));
	}
}

void Produce(int producerId, int value) override {
	slotsFree.acquire();


		{
		std::lock_guard < std::mutex > lock (mtx) ;
		queues[producerId].push(value);
	}

	itemsAvailable [producerId]->release();
}

int Consume(int producerId) override {
	itemsAvailable [producerId]->acquire();

        int value;

		{
		std::lock_guard < std::mutex > lock (mtx) ;
		value = queues[producerId].front();
		queues[producerId].pop();
	}

	slotsFree.release();
        return value;
}
};

class BufferMonitor : public AbstractBuffer {
private:
    std::mutex mtx;
std::condition_variable cv;
std::vector<std::queue<int>> queues;
int currentTotalItems = 0;

public:
    BufferMonitor(int cap, int prodCount) : AbstractBuffer(cap, prodCount) {
	queues.resize(prodCount + 1);
}

void Produce(int producerId, int value) override {
	std::unique_lock<std::mutex> lock(mtx);
	cv.wait(lock, [this]() { return currentTotalItems < capacity; });

	queues [producerId].push(value);
	currentTotalItems++;

	cv.notify_all();
}

int Consume(int producerId) override {
	std::unique_lock<std::mutex> lock(mtx);
	cv.wait(lock, [this, producerId]() { return !queues[producerId].empty(); });

        int value = queues [producerId].front();
	queues [producerId].pop();
	currentTotalItems--;

	cv.notify_all();
        return value;
}
};

int main()
{
	std::system("chcp 65001 > nul");

	int m = 3; 
	int k = 4; 
	std::vector<int> itemsCount = { 5, 4, 6 }; 

	std::cout << "ТЕСТУВАННЯ ВЕРСІЇ З СЕМАФОРАМИ\n";
	{
		BufferSemaphore buffer(k, m);

		std::vector<std::unique_ptr<Producer>> producers;
		std::vector<std::thread> threads;

		for (int i = 0; i < m; ++i)
		{
			producers.push_back(std::make_unique<Producer>(i + 1, itemsCount[i], std::ref (buffer)));
		}

		for (int i = 0; i < m; ++i)
		{
			threads.emplace_back(&Producer::Run, producers[i].get());
		}

		Consumer consumer(m, itemsCount, std::ref (buffer));
		threads.emplace_back(&Consumer::Run, &consumer);

		for (auto & t : threads) {
            t.join();
        }
    }

    std::cout << "\nТЕСТУВАННЯ ВЕРСІЇ З МОНІТОРАМИ\n";
{
	BufferMonitor buffer(k, m);
	std::vector<std::unique_ptr<Producer>> producers;
	std::vector<std::thread> threads;

	for (int i = 0; i < m; ++i)
	{
		producers.push_back(std::make_unique<Producer>(i + 1, itemsCount[i], std::ref (buffer)));
	}

	for (int i = 0; i < m; ++i)
	{
		threads.emplace_back(&Producer::Run, producers[i].get());
	}

	Consumer consumer(m, itemsCount, std::ref (buffer));
	threads.emplace_back(&Consumer::Run, &consumer);

	for (auto & t : threads)
	{
		t.join();
	}
}

std::cout << "\nМОДЕЛЮВАННЯ ЗАВЕРШЕНО УСПІШНО\n";
return 0;
}
