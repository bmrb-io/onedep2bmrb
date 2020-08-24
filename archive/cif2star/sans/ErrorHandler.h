/*
 * $Id: ErrorHandler.h 73 2006-08-09 18:38:31Z dmaziuk $
 *
 * This software is copyright (c) 2006 Board of Regents, University of Wisconsin.
 * All Rights Reserved.
 *
 */
#ifndef _ERROR_HANDLER_H_
#define _ERROR_HANDLER_H_

#include <string>
/**
 * \class ErrorHandler
 * \brief Interface that user code must implement to use sans parsers.
 *
 * This is a pure abstract class (equivalent of Java interface) that
 * contains error callback signatures for sans parsers.
 */
class ErrorHandler {
  public:
    /**
     * \brief Irrecoverable error.
     *
     * This callback is triggered by a critical error, such as i/o error, or
     * internal error in the lexer. It terminates the parser.
     *
     * \param line line number in the input file
     * \param msg error message text
     * \param col not used: column number
     */
    virtual void fatalError( int line, int col, const std::string & msg ) = 0;
    /**
     * \brief Parse error.
     *
     * This callback is triggered by a recoverable error. 
     * "Recoverable" errors are typically STAR syntax errors. It is up to user
     * code to decide whether to quit or continue parsing.
     *
     * \param line line number in the input file
     * \param msg error message text
     * \param col not used: column number
     * \return "stop" flag: if callback returns true, parser will stop and return
     */
    virtual bool error( int line, int col, const std::string & msg ) = 0;
    /**
     * \brief Parse warning.
     *
     * This callback is triggered by conditions that may or may not be errors,
     * for example:
     *  - loop count error: this is an error, but it does not affect the parser
     *  - STAR keyword or tag inside a delimited value: this mey be legitimate,
     *      or it may be due to missing closing delimiter
     * <p>
     * \param line line number in the input file
     * \param msg error message text
     * \param col not used: column number
     * \return "stop" flag: if callback returns true, parser will stop and return
     */
    virtual bool warning( int line, int col, const std::string & msg ) = 0;
    /**
     * To keep gcc happy.
     */
     virtual ~ErrorHandler() {}
};

#endif // ERROR_HANDLER_H
