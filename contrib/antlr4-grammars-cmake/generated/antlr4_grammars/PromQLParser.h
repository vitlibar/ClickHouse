
// Generated from PromQLParser.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"


namespace antlr4_grammars {


class  PromQLParser : public antlr4::Parser {
public:
  enum {
    NUMBER = 1, SCALAR = 2, STRING = 3, ADD = 4, SUB = 5, MULT = 6, DIV = 7, 
    MOD = 8, POW = 9, AND = 10, OR = 11, UNLESS = 12, EQ = 13, DEQ = 14, 
    NE = 15, GT = 16, LT = 17, GE = 18, LE = 19, RE = 20, NRE = 21, BY = 22, 
    WITHOUT = 23, ON = 24, IGNORING = 25, GROUP_LEFT = 26, GROUP_RIGHT = 27, 
    OFFSET = 28, BOOL = 29, AGGREGATION_OPERATOR = 30, FUNCTION = 31, LEFT_BRACE = 32, 
    RIGHT_BRACE = 33, LEFT_PAREN = 34, RIGHT_PAREN = 35, LEFT_BRACKET = 36, 
    RIGHT_BRACKET = 37, COMMA = 38, AT = 39, SUBQUERY_RANGE = 40, TIME_RANGE = 41, 
    DURATION = 42, METRIC_NAME = 43, LABEL_NAME = 44, WS = 45, SL_COMMENT = 46
  };

  enum {
    RuleExpression = 0, RuleVectorOperation = 1, RuleUnaryOp = 2, RulePowOp = 3, 
    RuleMultOp = 4, RuleAddOp = 5, RuleCompareOp = 6, RuleAndUnlessOp = 7, 
    RuleOrOp = 8, RuleVectorMatchOp = 9, RuleSubqueryOp = 10, RuleOffsetAt = 11, 
    RuleAtOp = 12, RuleOffsetOp = 13, RuleVector = 14, RuleParens = 15, 
    RuleInstantSelector = 16, RuleLabelMatcher = 17, RuleLabelMatcherOperator = 18, 
    RuleLabelMatcherList = 19, RuleRangeSelector = 20, RuleSelectorWithOffset = 21, 
    RuleFunction_ = 22, RuleParameter = 23, RuleParameterList = 24, RuleAggregation = 25, 
    RuleBy = 26, RuleWithout = 27, RuleGrouping = 28, RuleOn_ = 29, RuleIgnoring = 30, 
    RuleGroupLeft = 31, RuleGroupRight = 32, RuleLabelName = 33, RuleMetricName = 34, 
    RuleLabelNameList = 35, RuleKeyword = 36, RuleLiteral = 37
  };

  explicit PromQLParser(antlr4::TokenStream *input);

  PromQLParser(antlr4::TokenStream *input, const antlr4::atn::ParserATNSimulatorOptions &options);

  ~PromQLParser() override;

  std::string getGrammarFileName() const override;

  const antlr4::atn::ATN& getATN() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;


  class ExpressionContext;
  class VectorOperationContext;
  class UnaryOpContext;
  class PowOpContext;
  class MultOpContext;
  class AddOpContext;
  class CompareOpContext;
  class AndUnlessOpContext;
  class OrOpContext;
  class VectorMatchOpContext;
  class SubqueryOpContext;
  class OffsetAtContext;
  class AtOpContext;
  class OffsetOpContext;
  class VectorContext;
  class ParensContext;
  class InstantSelectorContext;
  class LabelMatcherContext;
  class LabelMatcherOperatorContext;
  class LabelMatcherListContext;
  class RangeSelectorContext;
  class SelectorWithOffsetContext;
  class Function_Context;
  class ParameterContext;
  class ParameterListContext;
  class AggregationContext;
  class ByContext;
  class WithoutContext;
  class GroupingContext;
  class On_Context;
  class IgnoringContext;
  class GroupLeftContext;
  class GroupRightContext;
  class LabelNameContext;
  class MetricNameContext;
  class LabelNameListContext;
  class KeywordContext;
  class LiteralContext; 

  class  ExpressionContext : public antlr4::ParserRuleContext {
  public:
    ExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VectorOperationContext *vectorOperation();
    antlr4::tree::TerminalNode *EOF();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ExpressionContext* expression();

  class  VectorOperationContext : public antlr4::ParserRuleContext {
  public:
    VectorOperationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    UnaryOpContext *unaryOp();
    std::vector<VectorOperationContext *> vectorOperation();
    VectorOperationContext* vectorOperation(size_t i);
    VectorContext *vector();
    PowOpContext *powOp();
    MultOpContext *multOp();
    AddOpContext *addOp();
    CompareOpContext *compareOp();
    AndUnlessOpContext *andUnlessOp();
    OrOpContext *orOp();
    SubqueryOpContext *subqueryOp();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  VectorOperationContext* vectorOperation();
  VectorOperationContext* vectorOperation(int precedence);
  class  UnaryOpContext : public antlr4::ParserRuleContext {
  public:
    UnaryOpContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ADD();
    antlr4::tree::TerminalNode *SUB();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  UnaryOpContext* unaryOp();

  class  PowOpContext : public antlr4::ParserRuleContext {
  public:
    PowOpContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *POW();
    GroupingContext *grouping();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  PowOpContext* powOp();

  class  MultOpContext : public antlr4::ParserRuleContext {
  public:
    MultOpContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MULT();
    antlr4::tree::TerminalNode *DIV();
    antlr4::tree::TerminalNode *MOD();
    GroupingContext *grouping();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  MultOpContext* multOp();

  class  AddOpContext : public antlr4::ParserRuleContext {
  public:
    AddOpContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ADD();
    antlr4::tree::TerminalNode *SUB();
    GroupingContext *grouping();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  AddOpContext* addOp();

  class  CompareOpContext : public antlr4::ParserRuleContext {
  public:
    CompareOpContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DEQ();
    antlr4::tree::TerminalNode *NE();
    antlr4::tree::TerminalNode *GT();
    antlr4::tree::TerminalNode *LT();
    antlr4::tree::TerminalNode *GE();
    antlr4::tree::TerminalNode *LE();
    antlr4::tree::TerminalNode *BOOL();
    GroupingContext *grouping();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CompareOpContext* compareOp();

  class  AndUnlessOpContext : public antlr4::ParserRuleContext {
  public:
    AndUnlessOpContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AND();
    antlr4::tree::TerminalNode *UNLESS();
    GroupingContext *grouping();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  AndUnlessOpContext* andUnlessOp();

  class  OrOpContext : public antlr4::ParserRuleContext {
  public:
    OrOpContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OR();
    GroupingContext *grouping();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  OrOpContext* orOp();

  class  VectorMatchOpContext : public antlr4::ParserRuleContext {
  public:
    VectorMatchOpContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ON();
    antlr4::tree::TerminalNode *UNLESS();
    GroupingContext *grouping();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  VectorMatchOpContext* vectorMatchOp();

  class  SubqueryOpContext : public antlr4::ParserRuleContext {
  public:
    SubqueryOpContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SUBQUERY_RANGE();
    OffsetAtContext *offsetAt();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SubqueryOpContext* subqueryOp();

  class  OffsetAtContext : public antlr4::ParserRuleContext {
  public:
    OffsetAtContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OffsetOpContext *offsetOp();
    AtOpContext *atOp();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  OffsetAtContext* offsetAt();

  class  AtOpContext : public antlr4::ParserRuleContext {
  public:
    AtOpContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AT();
    antlr4::tree::TerminalNode *SCALAR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  AtOpContext* atOp();

  class  OffsetOpContext : public antlr4::ParserRuleContext {
  public:
    OffsetOpContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *OFFSET();
    antlr4::tree::TerminalNode *SCALAR();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  OffsetOpContext* offsetOp();

  class  VectorContext : public antlr4::ParserRuleContext {
  public:
    VectorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Function_Context *function_();
    AggregationContext *aggregation();
    InstantSelectorContext *instantSelector();
    RangeSelectorContext *rangeSelector();
    SelectorWithOffsetContext *selectorWithOffset();
    LiteralContext *literal();
    ParensContext *parens();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  VectorContext* vector();

  class  ParensContext : public antlr4::ParserRuleContext {
  public:
    ParensContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LEFT_PAREN();
    VectorOperationContext *vectorOperation();
    antlr4::tree::TerminalNode *RIGHT_PAREN();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ParensContext* parens();

  class  InstantSelectorContext : public antlr4::ParserRuleContext {
  public:
    InstantSelectorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    MetricNameContext *metricName();
    antlr4::tree::TerminalNode *LEFT_BRACE();
    antlr4::tree::TerminalNode *RIGHT_BRACE();
    LabelMatcherListContext *labelMatcherList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  InstantSelectorContext* instantSelector();

  class  LabelMatcherContext : public antlr4::ParserRuleContext {
  public:
    LabelMatcherContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LabelNameContext *labelName();
    LabelMatcherOperatorContext *labelMatcherOperator();
    antlr4::tree::TerminalNode *STRING();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  LabelMatcherContext* labelMatcher();

  class  LabelMatcherOperatorContext : public antlr4::ParserRuleContext {
  public:
    LabelMatcherOperatorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EQ();
    antlr4::tree::TerminalNode *NE();
    antlr4::tree::TerminalNode *RE();
    antlr4::tree::TerminalNode *NRE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  LabelMatcherOperatorContext* labelMatcherOperator();

  class  LabelMatcherListContext : public antlr4::ParserRuleContext {
  public:
    LabelMatcherListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<LabelMatcherContext *> labelMatcher();
    LabelMatcherContext* labelMatcher(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  LabelMatcherListContext* labelMatcherList();

  class  RangeSelectorContext : public antlr4::ParserRuleContext {
  public:
    RangeSelectorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    InstantSelectorContext *instantSelector();
    antlr4::tree::TerminalNode *TIME_RANGE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  RangeSelectorContext* rangeSelector();

  class  SelectorWithOffsetContext : public antlr4::ParserRuleContext {
  public:
    SelectorWithOffsetContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    InstantSelectorContext *instantSelector();
    OffsetAtContext *offsetAt();
    RangeSelectorContext *rangeSelector();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SelectorWithOffsetContext* selectorWithOffset();

  class  Function_Context : public antlr4::ParserRuleContext {
  public:
    Function_Context(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FUNCTION();
    antlr4::tree::TerminalNode *LEFT_PAREN();
    antlr4::tree::TerminalNode *RIGHT_PAREN();
    std::vector<ParameterContext *> parameter();
    ParameterContext* parameter(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Function_Context* function_();

  class  ParameterContext : public antlr4::ParserRuleContext {
  public:
    ParameterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LiteralContext *literal();
    VectorOperationContext *vectorOperation();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ParameterContext* parameter();

  class  ParameterListContext : public antlr4::ParserRuleContext {
  public:
    ParameterListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LEFT_PAREN();
    antlr4::tree::TerminalNode *RIGHT_PAREN();
    std::vector<ParameterContext *> parameter();
    ParameterContext* parameter(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ParameterListContext* parameterList();

  class  AggregationContext : public antlr4::ParserRuleContext {
  public:
    AggregationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AGGREGATION_OPERATOR();
    ParameterListContext *parameterList();
    ByContext *by();
    WithoutContext *without();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  AggregationContext* aggregation();

  class  ByContext : public antlr4::ParserRuleContext {
  public:
    ByContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *BY();
    LabelNameListContext *labelNameList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ByContext* by();

  class  WithoutContext : public antlr4::ParserRuleContext {
  public:
    WithoutContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WITHOUT();
    LabelNameListContext *labelNameList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  WithoutContext* without();

  class  GroupingContext : public antlr4::ParserRuleContext {
  public:
    GroupingContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    On_Context *on_();
    IgnoringContext *ignoring();
    GroupLeftContext *groupLeft();
    GroupRightContext *groupRight();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  GroupingContext* grouping();

  class  On_Context : public antlr4::ParserRuleContext {
  public:
    On_Context(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ON();
    LabelNameListContext *labelNameList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  On_Context* on_();

  class  IgnoringContext : public antlr4::ParserRuleContext {
  public:
    IgnoringContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IGNORING();
    LabelNameListContext *labelNameList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  IgnoringContext* ignoring();

  class  GroupLeftContext : public antlr4::ParserRuleContext {
  public:
    GroupLeftContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *GROUP_LEFT();
    LabelNameListContext *labelNameList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  GroupLeftContext* groupLeft();

  class  GroupRightContext : public antlr4::ParserRuleContext {
  public:
    GroupRightContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *GROUP_RIGHT();
    LabelNameListContext *labelNameList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  GroupRightContext* groupRight();

  class  LabelNameContext : public antlr4::ParserRuleContext {
  public:
    LabelNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LABEL_NAME();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  LabelNameContext* labelName();

  class  MetricNameContext : public antlr4::ParserRuleContext {
  public:
    MetricNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *METRIC_NAME();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  MetricNameContext* metricName();

  class  LabelNameListContext : public antlr4::ParserRuleContext {
  public:
    LabelNameListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LEFT_PAREN();
    antlr4::tree::TerminalNode *RIGHT_PAREN();
    std::vector<LabelNameContext *> labelName();
    LabelNameContext* labelName(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  LabelNameListContext* labelNameList();

  class  KeywordContext : public antlr4::ParserRuleContext {
  public:
    KeywordContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *AND();
    antlr4::tree::TerminalNode *OR();
    antlr4::tree::TerminalNode *UNLESS();
    antlr4::tree::TerminalNode *BY();
    antlr4::tree::TerminalNode *WITHOUT();
    antlr4::tree::TerminalNode *ON();
    antlr4::tree::TerminalNode *IGNORING();
    antlr4::tree::TerminalNode *GROUP_LEFT();
    antlr4::tree::TerminalNode *GROUP_RIGHT();
    antlr4::tree::TerminalNode *OFFSET();
    antlr4::tree::TerminalNode *BOOL();
    antlr4::tree::TerminalNode *AGGREGATION_OPERATOR();
    antlr4::tree::TerminalNode *FUNCTION();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  KeywordContext* keyword();

  class  LiteralContext : public antlr4::ParserRuleContext {
  public:
    LiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SCALAR();
    antlr4::tree::TerminalNode *STRING();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  LiteralContext* literal();


  bool sempred(antlr4::RuleContext *_localctx, size_t ruleIndex, size_t predicateIndex) override;

  bool vectorOperationSempred(VectorOperationContext *_localctx, size_t predicateIndex);

  // By default the static state used to implement the parser is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:
};

}  // namespace antlr4_grammars
