#include <concepts>
#include <type_traits>

template <typename base_t, typename derived_t>
concept isDerived = std::is_base_of_v<base_t, derived_t>;

template <typename T>
concept BoolConvertible =  std::convertible_to<T, bool>;
