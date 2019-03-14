#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <ext/range.h>
#include <simdjson/jsonparser.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename Impl, bool ExtraArg>
class FunctionJSONBase : public IFunction
{
private:
    enum class Action
    {
        key = 1,
        index = 2,
    };

    mutable std::vector<Action> actions;
    mutable DataTypePtr virtual_type;

    bool tryMove(
        ParsedJson::iterator & pjh,
        Action action,
        const Field & accessor
    )
    {
        switch (action)
        {
            case Action::key:
                if (
                    !pjh.is_object()
                    || !pjh.move_to_key(accessor.get<String>().data())
                )
                    return false;

                break;
            case Action::index:
                if (
                    !pjh.is_object_or_array()
                    || !pjh.down()
                )
                    return false;

                int steps = accessor.get<Int64>();

                if (steps > 0)
                    steps -= 1;
                else if (steps < 0)
                {
                    steps += 1;

                    ParsedJson::iterator pjh1 {
                        pjh
                    };

                    while (pjh1.next())
                        steps += 1;
                }
                else
                    return false;

                for (const auto i : ext::range(0, steps))
                {
                    (void) i;

                    if (!pjh.next())
                        return false;
                }

                break;
        }

        return true;
    }

public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionJSONBase>();
    }

    String getName() const override
    {
        return Impl::name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if constexpr (ExtraArg)
        {
            if (arguments.size() < 2)
                throw Exception {
                    "Function " + getName() + " requires at least two arguments",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
                };

            virtual_type = arguments[1];
        }
        else
        {
            if (arguments.size() < 1)
                throw Exception {
                    "Function " + getName() + " requires at least one arguments",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
                };
        }

        if (!isString(arguments[0]))
            throw Exception {
                "Illegal type " + arguments[0]->getName()
                    + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
            };

        actions.reserve(arguments.size() - 1 - ExtraArg);

        for (const auto i : ext::range(1 + ExtraArg, arguments.size()))
        {
            if (isString(arguments[i]))
                actions.push_back(Action::key);
            else if (isInteger(arguments[i]))
                actions.push_back(Action::index);
            else
                throw Exception {
                    "Illegal type " + arguments[i]->getName()
                        + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
                };
        }

        if constexpr (ExtraArg)
            return Impl::getType(virtual_type);
        else
            return Impl::getType();
    }

    void executeImpl(
        Block & block,
        const ColumnNumbers & arguments,
        size_t result_pos,
        size_t input_rows_count
    ) override
    {
        MutableColumnPtr to {
            block.getByPosition(result_pos).type->createColumn()
        };
        to->reserve(input_rows_count);

        const ColumnPtr & arg_json = block.getByPosition(arguments[0]).column;

        for (const auto i : ext::range(0, input_rows_count))
        {
            // TODO: avoid multiple memory allocation?
            ParsedJson pj {
                build_parsed_json((*arg_json)[i].get<String>())
            };
            ParsedJson::iterator pjh {
                pj
            };

            bool ok = true;

            for (const auto j : ext::range(0, actions.size()))
            {
                ok = tryMove(
                    pjh,
                    actions[j],
                    (*block.getByPosition(arguments[j + 1 + ExtraArg]).column)[i]
                );

                if (!ok)
                    break;
            }

            if (ok)
            {
                if constexpr (ExtraArg)
                    to->insert(Impl::getValue(pjh, virtual_type));
                else
                    to->insert(Impl::getValue(pjh));
            }
            else
            {
                if constexpr (ExtraArg)
                    to->insert(Impl::getDefault(virtual_type));
                else
                    to->insert(Impl::getDefault());
            }
        }

        block.getByPosition(result_pos).column = std::move(to);
    }
};

}
