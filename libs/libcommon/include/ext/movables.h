#pragma once

#include <array>
#include <type_traits>

namespace ext
{
    /** Provides a replacement of std::initializer_list in case when elements are not copyable, but movable.
      * This template class is created because std::initializer_list requires elements to be copyable,
      * std::initializer_list cannot contain move-only objects.
      *
      * For example, ext::movables allows to iterate like that:
      *
      * for (auto x : ext::movables{std::make_unique<int>(5), std::make_unique<int>(7)})
      *     std::cout << "x=" << *x << std::endl;
      *
      * but if you try to compile:
      *
      * for (auto x : {std::make_unique<int>(5), std::make_unique<int>(7)})
      *     std::cout << "x=" << *x << std::endl;
      *
      * you'll get a compiler error.
      *
      * For further information see
      * http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2014/n4166
      * https://stackoverflow.com/questions/7231351/initializer-list-constructing-a-vector-of-noncopyable-but-movable-objects
      */
    template <typename ... Args>
    class movables
    {
        using internal_storage_type = std::array<std::common_type_t<Args ...>, sizeof...(Args)>;

    public:
        template <typename = std::enable_if_t<sizeof...(Args) >= 1, void>>
        movables(Args &&... args) { init<0>(std::move(args)...); }

        using value_type = typename internal_storage_type::value_type;
        using iterator = std::move_iterator<typename internal_storage_type::iterator>;
        using const_iterator = iterator;

        constexpr size_t size() const { return sizeof...(Args); }
        constexpr bool empty() const { return !size(); }
        iterator begin() { return std::make_move_iterator(storage.begin()); }
        iterator end() { return std::make_move_iterator(storage.end()); }

        /// Boost.Range which we really want to use doesn't support passing ranges by r-value reference.
        /// So const_cast<> here allows to use this class with Boost.Range.
        const_iterator begin() const { return const_cast<movables<Args...> *>(this)->begin(); }
        const_iterator end() const { return const_cast<movables<Args...> *>(this)->end(); }

    private:
        template <size_t i>
        void init() {}

        template <size_t i, typename FirstArg, typename ... OtherArgs>
        void init(FirstArg && first_arg, OtherArgs && ... other_args)
        {
            storage[i] = std::move(first_arg);
            init<i+1>(std::move(other_args) ...);
        }

        internal_storage_type storage;
    };
}
