#pragma once

#include <type_traits>

/**
 * @brief type trait for testing if a type is a specialization of template T
 */
template<template <typename...> typename T, typename U>
struct is_specialization : std::false_type {};

template <template <typename...> typename T, typename... Args>
struct is_specialization<T, T<Args...>> : std::true_type {};

template<typename Type, template <typename...> typename Template>
concept SpecializationOf = is_specialization<Template, std::decay_t<Type>>::value;

template<typename lambda_t, typename Ret, typename... Args>
concept NoThrowLambda = std::is_nothrow_invocable_r_v<lambda_t, Ret, Args...>;

template <typename T, typename... Args>
concept Constructible = std::is_constructible_v<T, Args...>;
