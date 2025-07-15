#ifndef S3_TRANSPORT_LOGGING_CATEGORY_HPP
#define S3_TRANSPORT_LOGGING_CATEGORY_HPP

#include "libs3/libs3.h"

#include <irods/irods_logger.hpp>

#include <fmt/format.h>

#include <type_traits>

// 1. Declare the custom category tag.
//    This structure does not need to define a body.
//    This tag allows the logger to locate data specific to the new category.
struct s3_transport_logging_category;

// 2. Specialize the logger configuration for the new category.
//    This also defines the default configuration for the new category.
namespace irods::experimental
{
    template <>
    class log::logger_config<s3_transport_logging_category>
    {
        // This defines the name that will appear in the log under the "log_category" key.
        // The "log_category" key defines where the message originated. Try to use a name
        // that makes it easy for administrators to determine what produced the message.
        static constexpr const char* name = "s3_transport_logging_category";

        // This is the current log level for the category. This also represents the initial
        // log level. Use the "set_level()" function to adjust the level.
        static inline log::level level = log::level::info;

        // This is required since the fields above are private.
        // This allows the logger to access and modify the configuration.
        friend class logger<s3_transport_logging_category>;
    };
} // namespace irods::experimental

template <>
struct fmt::formatter<S3Protocol> : fmt::formatter<std::underlying_type_t<S3Protocol>>
{
    constexpr auto format(const S3Protocol& e, format_context& ctx) const
    {
        return fmt::formatter<std::underlying_type_t<S3Protocol>>::format(
            static_cast<std::underlying_type_t<S3Protocol>>(e), ctx);
    }
};

template <>
struct fmt::formatter<S3UriStyle> : fmt::formatter<std::underlying_type_t<S3UriStyle>>
{
    constexpr auto format(const S3UriStyle& e, format_context& ctx) const
    {
        return fmt::formatter<std::underlying_type_t<S3UriStyle>>::format(
            static_cast<std::underlying_type_t<S3UriStyle>>(e), ctx);
    }
};

template <>
struct fmt::formatter<S3STSDate> : fmt::formatter<std::underlying_type_t<S3STSDate>>
{
    constexpr auto format(const S3STSDate& e, format_context& ctx) const
    {
        return fmt::formatter<std::underlying_type_t<S3STSDate>>::format(
            static_cast<std::underlying_type_t<S3STSDate>>(e), ctx);
    }
};

#endif //S3_TRANSPORT_LOGGING_CATEGORY_HPP
