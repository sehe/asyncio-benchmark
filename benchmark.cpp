#ifdef NDEBUG
#define SEHE_TWEAKS
#endif

// #define ASIO_ENABLE_HANDLER_TRACKING 1
#define ASIO_NO_DEPRECATED 1
#include <asio.hpp>
#include <iostream>
#include <syncstream>

using asio::ip::tcp;

constexpr std::string host     = "127.0.0.1";
constexpr uint16_t    port     = 12345;
constexpr size_t      FOUR_KiB = 4 * 1024;
static std::atomic_uint64_t g_recvd = 0, g_sent = 0;
static std::atomic_bool     g_session_shutdown = false;

static auto _cerr() { return std::osyncstream(std::cerr); }
#define errlog() _cerr() << "E " << __FUNCTION__ << ":" << __LINE__ << " "

#ifndef SEHE_TWEAKS
using Executor = asio::any_io_executor;
auto _cout() { return std::osyncstream(std::cout); }
    #define inflog() _cout() << "I " << __FUNCTION__ << ":" << __LINE__ << " "
#else // release
using Executor = asio::thread_pool::executor_type;
// static thread_local std::ostream s_nullstream{nullptr};
struct {
    template <typename T> constexpr auto& operator<<(T const&) const { return *this; }
    constexpr auto&                       operator<<(std::ostream& (*)(std::ostream&)) const { return *this; }
} static constexpr s_nullstream;
static auto& inflog() { return s_nullstream; }
#endif

namespace Client {
    void blasio(std::stop_token stopped, Executor ex, std::atomic_uint64_t& bytesRead) try {
        tcp::socket socket{ex};
        {
            tcp::resolver res{ex};
            connect(socket, res.resolve(host, std::to_string(port)));
        }

        std::vector<char> buf(FOUR_KiB);
        for (asio::error_code ec; !stopped.stop_requested() && !ec;)
            bytesRead += read(socket, asio::buffer(buf), ec);

        asio::error_code ignore_ec;
        socket.shutdown(tcp::socket::shutdown_both, ignore_ec);
    } catch (asio::system_error const& se) {
        if (se.code() != asio::error::eof)
            errlog() << se.code().message() << std::endl;
        else
            inflog() << "EOF from " << host << ":" << port << std::endl;
    }

    asio::awaitable<void, Executor> asio(std::atomic_uint64_t& bytesRead) try {
        Executor      ex = co_await asio::this_coro::executor;
        tcp::socket   socket{ex};

        tcp::resolver res{ex};
        co_await async_connect(socket, res.resolve(host, std::to_string(port)));

        for (std::vector<char> buf(FOUR_KiB);;)
            bytesRead += co_await async_read(socket, asio::mutable_buffer(buf.data(), buf.size()));

    } catch (asio::system_error const& se) {
        if (se.code() != asio::error::eof)
            errlog() << se.code().message() << std::endl;
        else
            inflog() << "EOF from " << host << ":" << port << std::endl;
    }

    static int blocking_connect() {
        int      sockfd = -1;
        addrinfo hints{};

        hints.ai_family   = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags    = 0; /// use default behavior
        hints.ai_protocol = 0;
        /// specifying 0 in this field indicates that socket addresses with any protocol can be
        /// returned by ::getaddrinfo();

        addrinfo* result = nullptr;
        if (auto rc = ::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &result); rc != 0) {
            errlog() << "Failed getaddrinfo with error: " << ::gai_strerror(rc) << std::endl;
            return false;
        }

        /// Try each address until we successfully connect
        for (auto rp = result; rp != nullptr; rp = rp->ai_next) {
            sockfd = ::socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
            if (sockfd == -1) {
                rp = rp->ai_next;
                continue;
            }

#ifndef SEHE_TWEAKS
            constexpr static timeval Timeout{0, 100'000};
            if (auto rc = ::setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &Timeout, sizeof(Timeout));
                    rc == -1) {
                errlog() << "Failed setsockopt with error: " << ::strerror(errno) << std::endl;
            } else
#endif
            {
                if (::connect(sockfd, rp->ai_addr, rp->ai_addrlen) != -1)
                    break; /// success
            }
            ::close(sockfd);
            sockfd = -1;
        }
        ::freeaddrinfo(result);

        return sockfd;
    }

    void blocking(std::stop_token stopped, std::atomic_uint64_t& bytesRead) {
        int sockfd = blocking_connect();
        if (sockfd == -1) {
            errlog() << "Could not connect to: " << host << ":" << port << std::endl;
            return;
        }

        std::string buf(FOUR_KiB,'\0');

        while (!stopped.stop_requested()) {
#ifndef SEHE_TWEAKS
            auto out = buf.data();
            for (size_t remain = FOUR_KiB; remain;) {
                switch (auto n = ::read(sockfd, out, remain)) {
                    case -1:
                        /// if read method returned -1 an error occurred during read.
                        errlog() << "Error reading from socket: " << strerror(errno)
                                 << std::endl;
                        remain = 0; // stop reading
                        break;
                    case 0:
                        inflog() << "EOF from " << host << ":" << port << std::endl;
                        remain = 0; // stop reading
                        [[fallthrough]];
                    default:
                        remain    -= n;
                        out       += n;
                        bytesRead += n;
                }
            }
#else
            if (auto n = ::read(sockfd, buf.data(), buf.size()); n > 0) {
                bytesRead += n;
            } else {
                if (!n) inflog() << "EOF from " << host << ":" << port << std::endl;
                else    errlog() << "Error reading from socket: " << strerror(errno) << std::endl;
                break;
            }
#endif
        }

        ::shutdown(sockfd, SHUT_RDWR);
        ::close(sockfd);
    }
} // namespace Client

namespace Server {
    asio::awaitable<void, Executor> session(tcp::socket socket) try {
        inflog() << "Connected " << socket.remote_endpoint() << std::endl;

        for (static std::vector const payload(FOUR_KiB, 'A'); !g_session_shutdown;) {
            auto [ec, n] = co_await async_write(socket, asio::buffer(payload), asio::as_tuple);
            g_sent      += n;

            if (ec) {
                socket.shutdown(tcp::socket::shutdown_both, ec);
                if (ec)
                    errlog() << "shutdown: " << ec.message() << std::endl;
                break;
            }
        }
    } catch (asio::system_error const& se) {
        errlog() << se.code().message() << std::endl;
    }

    asio::awaitable<void, Executor> listener() try {
        auto ex = co_await asio::this_coro::executor;

        for (tcp::acceptor acceptor{ex, {{}, port}};;)
            co_spawn(ex, session(co_await acceptor.async_accept()), asio::detached);
    } catch (asio::system_error const& se) {
        errlog() << se.code().message() << std::endl;
    }
} // namespace Server

#include <ranges> // split
#include <set>
#include <thread> // jthread
using namespace std::literals;

void stats(int elapsed_seconds) {
    errlog() << " -- " << std::endl;
    if (g_recvd)
        errlog() << "Total bytes read: " << g_recvd << " "                             //
                 << (g_recvd / 1024.0 / 1024.0 / 1024.0 / elapsed_seconds) << " GiB/s" //
                 << std::endl;
    if (g_sent)
        errlog() << "Total bytes sent: " << g_sent << " "                             //
                 << (g_sent / 1024.0 / 1024.0 / 1024.0 / elapsed_seconds) << " GiB/s" //
                 << std::endl;
}

asio::awaitable<void, Executor> stats_thread() {
    Executor ex = co_await asio::this_coro::executor;
    for (int i = 0; !g_session_shutdown; ++i) {
        if (i)
            stats(i);
        co_await asio::steady_timer(ex, 1s).async_wait();
    }
}

static inline auto now() { return std::chrono::steady_clock::now(); }
using std::this_thread::sleep_for;

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " [server|asio|blasio|blocking],... numConnections duration" << std::endl;
        return 1;
    }

    auto selection = [&] {
        auto rr = std::views::split(std::string_view(argv[1]), ',');
        return std::set<std::string_view>(rr.begin(), rr.end());
    }();

    auto const duration = std::max(1, std::stoi(argv[3]));

    auto start = now(), shutdown_time = start;
    {
        asio::cancellation_signal stop;

        auto         stoppable = asio::bind_cancellation_slot(stop.slot(), asio::detached);
        size_t const njobs     = std::stoi(argv[2]);

        asio::thread_pool         server_ctx(0), client_ctx(0);
        std::vector<std::jthread> threads;

        auto populate = [&threads](auto& pool, unsigned n) {
            while (n--)
                threads.emplace_back([&pool, i = threads.size()] {
                    cpu_set_t core{1ul << i};
                    pthread_setaffinity_np(pthread_self(), sizeof(core), &core);
                    pool.attach();
                });
        };
        populate(server_ctx, 4);
        populate(client_ctx, 4);

        Executor ex = server_ctx.get_executor();

        if (selection.contains("server"))
            co_spawn(ex, Server::listener(), stoppable);

        sleep_for(10ms); // allow server to start
        // co_spawn(ex, stats_thread, asio::detached);

        if (selection.contains("asio"))
            for (size_t i = 0; i < njobs; ++i)
                co_spawn(client_ctx, Client::asio(g_recvd), asio::detached);
        if (selection.contains("blocking"))
            generate_n(back_inserter(threads), njobs,
                       [&] { return std::jthread(Client::blocking, std::ref(g_recvd)); });
        if (selection.contains("blasio"))
            generate_n(back_inserter(threads), njobs, [&] {
                return std::jthread(Client::blasio, client_ctx.get_executor(), std::ref(g_recvd));
            });

        errlog() << "Running " << threads.size() << " threads for " << duration << " seconds" << std::endl;

        sleep_for(1s * duration);

        shutdown_time = now();
        stop.emit(asio::cancellation_type::all);
        g_session_shutdown = true;

        server_ctx.join(); // allow asio operations to clean up
        client_ctx.join();
    }
    errlog() << "Total duration: " << (now() - start) / 1.s << "s "
             << "(shutdown took " << (now() - shutdown_time) / 1ms << "ms)" << std::endl;

    stats(duration);
}
