#pragma once

#include <Core/Defines.h>
#include <Parsers/Lexer.h>

#include <cassert>
#include <vector>


namespace DB
{

/** Parser operates on lazy stream of tokens.
  * It could do lookaheads of any depth.
  */

/** Used as an input for parsers.
  * All whitespace and comment tokens are transparently skipped if `skip_insignificant`.
  */
class Tokens
{
private:
    std::vector<Token> data;
    size_t max_pos = 0;
    Lexer lexer;
    bool skip_insignificant;

public:
    Tokens(const char * begin, const char * end, size_t max_query_size = 0, bool skip_insignificant_ = true)
        : lexer(begin, end, max_query_size), skip_insignificant(skip_insignificant_)
    {
    }

    const Token & operator[] (size_t index)
    {
        while (true)
        {
            if (index < data.size())
            {
                max_pos = std::max(max_pos, index);
                return data[index];
            }

            if (!data.empty() && data.back().isEnd())
            {
                max_pos = data.size() - 1;
                return data.back();
            }

            Token token = lexer.nextToken();

            if (!skip_insignificant || token.isSignificant())
                data.emplace_back(token);
        }
    }

    /// Rightmost token we had looked.
    const Token & max()
    {
        if (data.empty())
            return (*this)[0];
        return data[max_pos];
    }

    void resetMax()
    {
        max_pos = 0;
    }

    /// Replaces a parsed token with a manually adjusted token with its type or the end position changed.
    void adjustToken(size_t index, const Token & new_token)
    {
        Token old_token = (*this)[index];
        data[index].type = new_token.type;
        if (new_token.end != old_token.end)
        {
            const char * new_end = std::max(old_token.begin, std::min(new_token.end, getEndOfStream()));
            data[index].end = new_end;
            data.erase(data.begin() + index + 1, data.end());
            max_pos = index;
            lexer.setPosition(new_end);
        }
    }

    /// Returns the end of the input stream.
    const char * getEndOfStream() const { return lexer.getEnd(); }
};


/// To represent position in a token stream.
class TokenIterator
{
private:
    Tokens * tokens;
    size_t index = 0;

public:
    explicit TokenIterator(Tokens & tokens_) : tokens(&tokens_) {}

    ALWAYS_INLINE const Token & get() { return (*tokens)[index]; }
    ALWAYS_INLINE const Token & operator*() { return get(); }
    ALWAYS_INLINE const Token * operator->() { return &get(); }

    ALWAYS_INLINE TokenIterator & operator++()
    {
        ++index;
        return *this;
    }
    ALWAYS_INLINE TokenIterator & operator--()
    {
        --index;
        return *this;
    }

    ALWAYS_INLINE bool operator<(const TokenIterator & rhs) const { return index < rhs.index; }
    ALWAYS_INLINE bool operator<=(const TokenIterator & rhs) const { return index <= rhs.index; }
    ALWAYS_INLINE bool operator==(const TokenIterator & rhs) const { return index == rhs.index; }
    ALWAYS_INLINE bool operator!=(const TokenIterator & rhs) const { return index != rhs.index; }

    ALWAYS_INLINE bool isValid() { return get().type < TokenType::EndOfStream; }

    /// Rightmost token we had looked.
    ALWAYS_INLINE const Token & max() { return tokens->max(); }

    /// Replaces a parsed token with a manually adjusted token with its type or the end position changed.
    ALWAYS_INLINE void adjustToken(const Token & new_token) { tokens->adjustToken(index, new_token); }

    /// Returns the end of the input stream.
    ALWAYS_INLINE const char * getEndOfStream() const { return tokens->getEndOfStream(); }
};


/// Returns positions of unmatched parentheses.
using UnmatchedParentheses = std::vector<Token>;
UnmatchedParentheses checkUnmatchedParentheses(TokenIterator begin);

}
