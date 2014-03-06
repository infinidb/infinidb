/* Copyright (C) 2014 InfiniDB, Inc.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/***********************************************************************
*   $Id: exp_templates.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef EXP_TEMPLATES
#define EXP_TEMPLATES

#include <iostream>
#include <fstream>
#include <stack>
#include <stdexcept>
#include <cassert>
#include <string>
#include <iterator>
//#include <parsetree.h>

namespace expression {
   enum precedence { none, lower, equal, higher }; 

  struct not_an_acceptor_tag {};
  struct acceptor_tag {};
  struct accumulating_acceptor_tag {};

  template<typename Acceptor>
  struct acceptor_traits {
    typedef typename Acceptor::acceptor_category acceptor_category;
    typedef typename Acceptor::input_symbol_type input_symbol_type;
    typedef typename Acceptor::result_type       result_type;
  };

    // Constants indicating possible operator placements. An incoming operator
    // may be ambiguous so its position will be the bitwise or of several of 
    // these positions.
    const int prefix         = 0x01;
    const int postfix        = 0x02;
    const int infix          = 0x04;
    const int open           = 0x08;
    const int close          = 0x10;
    const int function_open  = 0x20;
    const int function_close = close;
    
    enum associativity { non_associative, left_associative, right_associative };
    
    struct default_expression_parser_error_policy {
        template<typename Operand>
        void invalid_operand_position(Operand)
        {
          std::cerr << "Invalid operand position\n";
        }
        
        template<typename Operator>
        void invalid_operator_position(Operator)
        {
          std::cerr << "Invalid operator position\n";
        }
        
        template<typename Operator>
        void requires_precedence_relation(Operator, Operator)
        {
          std::cerr << "Requires precedence relation\n";
        }
        
        template<typename Operator>
        void requires_associativity_relation(Operator, Operator)
        {
          std::cerr << "Requires associativity relation\n";
        }
        
        template<typename Operator>
        void ambiguity(Operator, Operator)
        {
          std::cerr << "Unresolvable ambiguity\n";
        }
        
        template<typename Operator>
        void unbalanced_confix(Operator)
        {
          std::cerr << "Unbalanced confix operator\n";
        }
        
        template<typename Operator>
        void missing_operand(Operator)
        {
          std::cerr << "Missing operand\n";
        }
    };
    
#define EXP_TEMPLATE \
  template<typename Token , typename Operand, typename Operator,           \
           typename Policy, typename OperandStack, typename OperatorStack>

#define EXP_ACCEPTOR \
    expression_acceptor<Token, Operand, Operator, Policy, OperandStack, \
                        OperatorStack>
    
    namespace detail {
        enum action {shift, reduce, prec, prec_assoc, invalid};
        
        // Current larser state    
        enum state { accepting, rejected, pre_operand, post_operand, lookahead };
        
        // The operator placements that are allowed for each state
        const int pre_positions  = prefix | open;
        const int post_positions = postfix | close | infix | function_open;
        
        const action parse_action[6][6] = { 
                 /* prefix   postfix infix       open    close   func open */
/* prefix    */ { shift,   prec,   prec,       shift,  reduce, prec   },
/* postfix   */ { invalid, reduce, reduce,     reduce, reduce, reduce },
/* infix     */ { shift,   prec,   prec_assoc, shift,  reduce, prec   },
/* open      */ { shift,   shift,  shift,      shift,  shift,  shift  },
/* close     */ { reduce,  reduce, reduce,     reduce, reduce, reduce },
/* func open */ { shift,   shift,  shift,      shift,  shift,  shift  }
      };

    template<
      typename Token,
      typename Operand, 
      typename Operator, 
      typename Policy,
      typename OperandStack = std::stack<Operand>,
      typename OperatorStack = std::stack<Operator>
    >
    class expression_acceptor {
    private:
        class assignment_proxy {
        public:
            explicit assignment_proxy(expression_acceptor* acceptor) : 
              m_expression_acceptor(acceptor) {}
            
            assignment_proxy& operator=(const Token& t)
            {
              m_expression_acceptor->disambiguate_and_parse(t);
              return *this;
            }
          
        private:
            expression_acceptor* m_expression_acceptor;
        };
        
        friend class assignment_proxy;

    public:
        typedef accumulating_acceptor_tag acceptor_category;
        typedef Token input_symbol_type;
        typedef Operand result_type;
        typedef std::output_iterator_tag iterator_category;
        typedef Token                    value_type;
        typedef Token&                   reference;
        typedef Token*                   pointer;
        typedef int                      difference_type;
        
        explicit expression_acceptor(const Policy& policy, 
                                     OperandStack& operandStack,
                                     OperatorStack& operatorStack) : 
          m_policy(policy), 
          m_operand_stack (operandStack),
          m_operator_stack (operatorStack),
          m_state(detail::pre_operand)
        {
        }
        
        assignment_proxy     operator*()       { return assignment_proxy(this); }
        expression_acceptor& operator++()      { return *this; }
        expression_acceptor& operator++(int)   { return *this; }
        
        bool accepted();
        bool trapped() { return m_state == rejected; }
        
        operator Operand()
        {
          return this->accepted()? m_operand_stack.top() : Operand();
        }
        
    private:
        void disambiguate_and_parse(const Token&);
        int operator_type_index(int);
        void parse_operator(const Operator&);
        void do_reduce();      
    
    private:
        Policy        m_policy;
        OperandStack&  m_operand_stack; // changed to reference by Zhixuan Zhu
        OperatorStack& m_operator_stack; // changed to reference by Zhixuan Zhu        
        state         m_state;
        Token         m_ambiguous_operator;
    };    

    EXP_TEMPLATE
    bool EXP_ACCEPTOR::accepted()
    {
        if (m_state == accepting) {
          return true;
        }
        else if (m_state == rejected) {
          return false;
        }
        else if (m_state == lookahead) {
          m_state = post_operand;
          int position = m_policy.positions(m_ambiguous_operator);
          parse_operator(m_policy.as_operator(m_ambiguous_operator,
                                             position & (postfix | close)));
        }
        
        while (!m_operator_stack.empty() && m_state != rejected) {
          do_reduce();
        }
        
        if (m_state == rejected) {
          return false;
        }
        else if (m_operand_stack.size() > 1 || !m_operator_stack.empty()) {
          m_state = rejected;
          return false;
        }
        else {
          m_state = accepting;
          return true;
        }
    }

    EXP_TEMPLATE
    void EXP_ACCEPTOR::disambiguate_and_parse(const Token& t)
    {
        assert(m_state != accepting);
        
        if (m_state == rejected)
          return;
        
        if (m_policy.is_operator(t)) {
            int operator_positions = m_policy.positions(t);
            
            if (m_state == lookahead) {
              bool could_be_pre = ((operator_positions & pre_positions) != 0);
              bool could_be_post = ((operator_positions & post_positions) != 0);
            
              int ambiguous_position = m_policy.positions(m_ambiguous_operator);
            
              if ( (could_be_pre && could_be_post) ||
                  (!could_be_pre && !could_be_post) ) {
                m_policy.ambiguity(m_ambiguous_operator, t);
                m_state = rejected;
                return;
              }
              else if (could_be_pre) {
                ambiguous_position &= (infix | function_open);
                parse_operator(m_policy.as_operator(m_ambiguous_operator, 
                                                   ambiguous_position));
                m_state = pre_operand;
              }
              else {
                ambiguous_position &= (postfix | close);
                parse_operator(m_policy.as_operator(m_ambiguous_operator, 
                                                   ambiguous_position));
                m_state = post_operand;
              }
            }
            
            int valid_positions = 
              (m_state == pre_operand)? pre_positions : post_positions;
            
            int positions = operator_positions & valid_positions;
            switch (positions) {
            case infix:
            case function_open:
              m_state = pre_operand;
            
              // Fall through
            case prefix:
            case postfix:
            case open:
            case close:
              parse_operator(m_policy.as_operator(t, positions));
              break;
            
            case postfix | infix:
            case close | infix:
            case postfix | function_open:
            case function_open | function_close:
              m_state = lookahead;
              m_ambiguous_operator = t;
              break;
            
            default:
              m_policy.invalid_operator_position(t);
              m_state = rejected;
              return;
            }
        }
        else {
            if (m_state == lookahead) {
              int positions = m_policy.positions(m_ambiguous_operator);
              positions &= (infix | function_open);
              parse_operator(m_policy.as_operator(m_ambiguous_operator, 
                                                  positions));
              m_state = post_operand;
            }
            else if (m_state == post_operand) {
              m_policy.invalid_operand_position(m_policy.as_operand(t));
              m_state = rejected;
              return;
            }
            else {
              m_state = post_operand;
            }
            
            m_operand_stack.push(m_policy.as_operand(t));
        }
    }

    EXP_TEMPLATE
    inline int EXP_ACCEPTOR::operator_type_index(int position)
    {
        switch (position) {
        case prefix:
          return 0;
        
        case postfix:
          return 1;
        
        case infix:
          return 2;
        
        case open:
          return 3;
        
        case close:
          return 4;
        
        case function_open:
          return 5;
        }
        
        assert(0);
        return 0;
    }

    EXP_TEMPLATE
    void EXP_ACCEPTOR::parse_operator(const Operator& cur_operator)
    {
        if (m_operator_stack.empty()) {
          m_operator_stack.push(cur_operator);
        }
        else {
            int cur_index = operator_type_index(m_policy.position(cur_operator));
            
            Operator& prev_operator = m_operator_stack.top();
            int prev_index = operator_type_index(m_policy.position(prev_operator));
            
            switch (parse_action[prev_index][cur_index]) {
            case shift:
              m_operator_stack.push(cur_operator);
              break;
	        
            case reduce:
              do_reduce();
              parse_operator(cur_operator);
              break;
            
            case prec:
              switch (m_policy.precedence(prev_operator, cur_operator)) {
              case lower:
                m_operator_stack.push(cur_operator);
                break;
            
              case higher:
                do_reduce();
                parse_operator(cur_operator);
                break;
            
              default:
                m_policy.requires_precedence_relation(prev_operator, cur_operator);
                m_state = rejected;
                return;
              }
              break;
            
            case prec_assoc:
                switch (m_policy.precedence(prev_operator, cur_operator)) {
                case lower:
                  m_operator_stack.push(cur_operator);
                  break;
                
                case higher:
                  do_reduce();
                  parse_operator(cur_operator);
                  break;
                
                case equal:
                  switch(m_policy.associativity(prev_operator, cur_operator)) {
                  case left_associative:
                    do_reduce();
                    parse_operator(cur_operator);
                    break;
                
                  case right_associative:
                    m_operator_stack.push(cur_operator);
                    break;
                
                  default:
                    m_policy.requires_associativity_relation(prev_operator, 
                                                           cur_operator);
                    m_state = rejected;
                    return;
                  }
                  break;
                
                default:
                  m_policy.requires_precedence_relation(prev_operator, cur_operator);
                  m_state = rejected;
                  return;
                }
                break;
                
            case invalid:
                m_policy.invalid_operator_position(cur_operator);
                m_state = rejected;
                return;
            }
        }
    }

    EXP_TEMPLATE
    void EXP_ACCEPTOR::do_reduce()
    {
      Operator op = m_operator_stack.top();
      m_operator_stack.pop();
    
      switch (m_policy.position(op)) {
      case prefix:
      case postfix:
        {
          if (m_operand_stack.empty()) {
            m_policy.missing_operand(op);
            m_state = rejected;
            return;
          }

          Operand operand = m_operand_stack.top();
          m_operand_stack.pop();

          m_operand_stack.push(m_policy.reduce(op, operand));
        }
        break;

      case infix:
        {
          if (m_operand_stack.size() < 2) {
            m_policy.missing_operand(op);
            m_state = rejected;
            return;
          }

          Operand rhs = m_operand_stack.top();
          m_operand_stack.pop();

          Operand lhs = m_operand_stack.top();
          m_operand_stack.pop();

          m_operand_stack.push(m_policy.reduce(op, lhs, rhs));    
        }
        break;

      case open:
        m_policy.unbalanced_confix(op);
        m_state = rejected;
        return;

      case close:
        if (m_operator_stack.empty()) {
          m_policy.unbalanced_confix(op);
          m_state = rejected;
          return;
        }

        if (m_operand_stack.empty()) {
          m_policy.missing_operand(op);
          m_state = rejected;
          return;
        }

        {
          Operator open = m_operator_stack.top();
          m_operator_stack.pop();

          Operand operand = m_operand_stack.top();
          m_operand_stack.pop();

          if (m_policy.position(open) == function_open) {
            if (m_operand_stack.empty()) {
              m_policy.missing_operand(open);
              m_state = rejected;
              return;
            }

            Operand function = m_operand_stack.top();
            m_operand_stack.pop();

            m_operand_stack.push(
              m_policy.reduce(function, open, operand, op)
            );
          }
          else {
            m_operand_stack.push(m_policy.reduce(open, op, operand));
          }
        }
        return;

      default:
        assert(0);
      }
    }
  }

  EXP_TEMPLATE
  inline bool accepted(const detail::EXP_ACCEPTOR& acceptor)
  {
    return acceptor.accepted();
  }

  EXP_TEMPLATE
  inline bool trapped(const detail::EXP_ACCEPTOR& acceptor)
  {
    return acceptor.trapped();
  }

  template<
    typename Token,
    typename Operand, 
    typename Operator, 
    typename Policy,
    typename OperandStack = std::stack<Operand>,
    typename OperatorStack = std::stack<Operator>
  >
  class expression_parser {
  public:
    typedef detail::EXP_ACCEPTOR acceptor;
    typedef Token token_type;
    typedef Operand operand_type;
    typedef Operator operator_type;
    typedef Policy policy_type;
    typedef Token input_symbol_type;
    typedef Operand result_type;

    expression_parser() : m_policy() {}
    explicit expression_parser(const Policy& policy) : m_policy(policy) {}

    template<typename InputIterator> 
    Operand parse(InputIterator first, InputIterator last)
    {
        try {
#if _MSC_VER > 1600 
            return std::_Copy_impl(first, last, start());}
#else
            return std::copy(first, last, start());}
#endif
        catch (const std::runtime_error&) {
            m_policy.cleanup(operandStack, operatorStack);
            throw;
        }
    }

    acceptor start() 
    {
        // pass in operator and operand stacks to acceptor constructor.
        // this is to chean up the pointers in the stack when exception
        // is thrown. the exception is then passed to the upper level.
        // changed by Zhixuan Zhu 07/03/06
        try { 
            return acceptor(m_policy, operandStack, operatorStack); 

        } catch (const std::runtime_error&)
        {
            m_policy.cleanup(operandStack, operatorStack);
            throw;
        }
    }

  private:
    Policy m_policy;
    
    // added operand and operator stacks by Zhixuan Zhu
    OperandStack operandStack;
    OperatorStack operatorStack;
  };

#undef EXP_ACCEPTOR
#undef EXP_TEMPLATE
}

#endif

