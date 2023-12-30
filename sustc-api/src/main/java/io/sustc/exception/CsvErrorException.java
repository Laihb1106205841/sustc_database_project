package io.sustc.exception;

public class CsvErrorException extends RuntimeException{
    public CsvErrorException(){}
    public CsvErrorException(String message){super(message);}

    public CsvErrorException(Throwable cause){super(cause);}
}
