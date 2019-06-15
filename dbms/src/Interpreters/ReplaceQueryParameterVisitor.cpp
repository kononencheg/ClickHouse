#include <boost/algorithm/string/replace.hpp>
#include <Common/typeid_cast.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryParameter.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Interpreters/addTypeConversionToAST.h>


namespace DB
{

void ReplaceQueryParameterVisitor::visit(ASTPtr & ast)
{
    for (auto & child : ast->children)
    {
        if (child->as<ASTQueryParameter>())
            visitQueryParameter(child);
        else
            visit(child);
    }
}

const String & ReplaceQueryParameterVisitor::getParamValue(const String & name)
{
    auto search = parameters_substitution.find(name);
    if (search != parameters_substitution.end())
        return search->second;
    else
        throw Exception("Expected name '" + name + "' in argument --param_{name}", ErrorCodes::BAD_ARGUMENTS);
}

void ReplaceQueryParameterVisitor::visitQueryParameter(ASTPtr & ast)
{
    const auto & ast_param = ast->as<ASTQueryParameter &>();
    const String & value = getParamValue(ast_param.name);
    const String & type_name = ast_param.type;

    const auto data_type = DataTypeFactory::instance().get(type_name);
    auto temp_column_ptr = data_type->createColumn();
    IColumn & temp_column = *temp_column_ptr;
    ReadBufferFromString read_buffer{value};
    FormatSettings format_settings;
    data_type->deserializeAsWholeText(temp_column, read_buffer, format_settings);

    if (!read_buffer.eof())
        throw Exception("Value " + value + " cannot be parsed as " + type_name + " for query parameter '"  + ast_param.name + "'", ErrorCodes::BAD_ARGUMENTS);

    ast = addTypeConversionToAST(std::make_shared<ASTLiteral>(temp_column[0]), type_name);
}

}