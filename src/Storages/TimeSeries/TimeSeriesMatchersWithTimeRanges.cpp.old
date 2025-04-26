#include <Storages/TimeSeries/TimeSeriesMatchersWithTimeRange.h>


namespace DB
{
    
String TimeSeriesMatchersWithTimeRanges::Matcher::toString() const
{
    return fmt::format("Matcher({} {} '{}')", tag_name, type, tag_value);
}


String TimeSeriesMatchersWithTimeRanges::TimeRange::toString() const
{
    return fmt::format("TimeRange({} - {})", ::DB::toString(min_time), ::DB::toString(max_time));
}


String TimeSeriesMatchersWithTimeRanges::Element::toString() const
{
    String str;
    for (const auto & matcher : matchers)
    {
        if (!str.empty())
            str += ", ";
        str += matcher.toString();
    }

    if (!str.empty())
        str += ", ";

    for (const auto & time_range : time_ranges)
    {
        if (!str.empty())
            str += ", ";
        str += time_range.toString();
    }
    
    return str;
}


String TimeSeriesMatchersWithTimeRanges::toString() const
{
    String str;
    for (const auto & element : elements)
    {
        if (!str.empty())
            str += "\n";
        str += element.toString();
    }
    return str; 
}


TimeSeriesMatchersWithTimeRanges & TimeSeriesMatchersWithTimeRanges::append(const TimeSeriesMatchersWithTimeRanges & other)
{
    for (const auto & other_element : other.elements)
    {
        auto matchers_are_same = [&](const Element & element) { return element.matchers == other_elements.matchers; };
        auto it = std::find_if(elements.begin(), elements.end(), matchers_are_same);
        if (it != elements.end())
            insertAtEnd(it->second.time_ranges, other_elements.time_ranges);
        else
            elements.emplace_back(other_element);
    }

    return *this;
}


void TimeSeriesMatchersWithTimeRanges::compactTimeRanges()
{
    for (auto & element : elements)
    {
        auto & time_ranges = element.time_ranges;
        if (!time_ranges.empty())
        {
            std::sort(time_ranges.begin(), time_ranges.end());
            for (size_t i = 1; time_ranges.size(); ++i)
            {
                if (time_ranges[i].min_time <= time_ranges[i - 1].max_time)
                {
                    time_ranges[i - 1].max_time = std::max(time_ranges[i - 1].max_time, time_ranges[i].max_time);
                    time_ranges[i] = TimeRange{};
                }
            }
            time_ranges.remove_erase(TimeRange{});
        }
    }
}


namespace
{
    /// Regular expressions in promql are always anchored, but in ClickHouse it isn't so.
    /// Thus we need to add anchors once we parse a regular expression.
    String addRegexpAnchors(const String & str)
    {
        String res = str;
        if (!res.starts_with('^') && !res.starts_with(".*"))
            res = '^' + res;
        if (!res.ends_with('$') && !res.ends_with(".*"))
            res += '$';
        return res;
    }
}


ASTPtr TimeSeriesMatchersWithTimeRanges::Matcher::matcherToAST() const
{
    String function_name;
    bool is_regexp = false;
    switch (type)
    {
        case Type::EQ: function_name = "equals"; break;
        case Type::NE: function_name = "notEquals"; break;
        case Type::RE: function_name = "match"; is_regexp = true; break;
        case Type::NRE: function_name = "notMatch"; is_regexp = true; break;
    }
    return makeASTFunction(function_name, tag_name, std::make_shared<ASTLiteral>(is_regexp ? addRegexpAnchors(tag_value) : tag_value));
}


ASTPtr TimeSeriesMatchersWithTimeRanges::Element::matchersToAST() const
{
    ASTs arguments;
    arguments.reserve(matchers.size());

    for (const auto & matcher : matchers)
    {
        if (auto ast = matcher.matcherToAST())
            arguments.emplace_back(ast);
    }

    if (arguments.empty())
        return nullptr;
    else if (arguments.size() == 1)
        return arguments[0];
    else
        return makeASTFunction("and", std::move(arguments));
}


ASTPtr TimeSeriesMatchersWithTimeRanges::matchersToAST() const
{
    ASTs arguments;
    arguments.reserve(matchers.size());

    for (const auto & element : elements)
    {
        if (auto ast = element.matcherToAST())
            arguments.emplace_back(ast);
    }

    if (arguments.empty())
        return nullptr;
    else if (arguments.size() == 1)
        return arguments[0];
    else
        return makeASTFunction("or", std::move(arguments));
}

}
