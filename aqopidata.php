<?php

require_once(dirname(__FILE__).'/aqopi2finsave.php');

require_once(dirname(__FILE__).'/api.php');

class AqopiData extends Api {
    const SUPPORTED_COLUMN_TYPES = ["boolean"=>"i","integer"=>"i","double"=>"d","string"=>"s"];
    
    private $userHash;
    private $headerID;
    private $tableColumnList;
    
    private function __construct($connection, $userHash, $headerID) {
        parent::__construct($connection, '');
        $this->userHash = $userHash;
        $this->headerID = $headerID;
    } 
    
    private function isSupported($columnValue) {
        return array_key_exists(gettype($columnValue), self::SUPPORTED_COLUMN_TYPES);  
    }
    
    private function getParameterType($columnValue) {
        if (!$this->isSupported($columnValue)) {
            throw new Exception('003 ' . "$columnValue is not suppported");  
        }
        return self::SUPPORTED_COLUMN_TYPES[gettype($columnValue)];
    }
    
    private function refreshTableColumnList($tableName) {
        $this->tableColumnList = array();
        $sql = "
            select column_name 
              from information_schema.columns 
             where table_name like concat('', ?, '')
               and table_schema like 'aqopiapp'
        ";           
        $statement = $this->connection->prepare($sql);   
        if (!$statement) {
            throw new Exception('003 ' . $this->connection->error);     
        }
        $statement->bind_param(
          's'
        , $tableName
        );    
        if  (!$statement) {
            throw new Exception('003 ' . $this->connection->error);    
        }
        if (!$statement->execute()) {
            throw new Exception('003 ' . $this->connection->error);
        }
        $statement->bind_result(
            $columnName
        );        
        while ($statement->fetch()) {
            $this->tableColumnList[] = $columnName;
        }         
        $statement->close();
    }
    
    private function isAvailable($tableName, $columnName) {                 
        return in_array($columnName, $this->tableColumnList);  
    } 
    
    private function addRecordItemData($tableName, $record, $parentColumn = null, $parentValue = null) {
        $result = 0;
        $array_params = array();
        $sql_insert = "
            insert
              into aqopiapp.$tableName
        ";
        $sql_columns = "
                 ( header_rid
        ";
        $sql_parameters = "
                 )
            select ?
        ";
        $parameterTypes = "i";    
        $array_params[] = &$parameterTypes; // pass by reference
        $array_params[] = &$this->headerID; // add reference to first parameterValue
        if (is_array($record)) {
            foreach ($record as $columnName => $columnValue) {
                if (    (!is_null($columnValue)) 
                     && ($this->isSupported($columnValue))
                     && ($this->isAvailable($tableName, $columnName))
                   ) {                
                    $sql_columns .= "
                         , $columnName
                    ";
                    $sql_parameters .= "
                         , ?
                    ";
                    if (gettype($columnValue) == 'string') {
                        $columnValue = htmlspecialchars_decode($columnValue); 
                        // strings may contain \u00e9 which must be converted to é
                        $columnValue = preg_replace('/\\\\u([\da-fA-F]{4})/', '&#x\1;', $columnValue); 
                        $columnValue = html_entity_decode($columnValue);
                        $record[$columnName] = $columnValue;
                    }
                    $parameterTypes .= $this->getParameterType($columnValue);
                    $array_params[] = &$record[$columnName]; // add reference to $columnValue
                }
            }
            if (($parentColumn) && ($parentValue)) {
                if (    ($this->isSupported($parentValue))
                     && ($this->isAvailable($tableName, $parentColumn))
                   ) {
                    $sql_columns .= "
                         , $parentColumn
                    ";
                    $sql_parameters .= "
                         , ?
                    ";
                    $parameterTypes .= $this->getParameterType($parentValue);
                    $array_params[] = &$parentValue; 
                } else {
                    throw new Exception('003 ' . "Unable to insert childrecord in $tableName with value $parentValue in parentcolumn $parentColumn");    
                }
            }        
            $sql = $sql_insert . $sql_columns . $sql_parameters;      
            $statement = $this->connection->prepare($sql);  
            if (!$statement) {
                throw new Exception('003 ' . "$sql is not correct sql");
            }
            if (!call_user_func_array(array($statement, 'bind_param'), $array_params)) {
                throw new Exception('003 ' . implode(" ",$array_params));
            }
            if (!$statement->execute()) {    
                throw new Exception('003 ' . $this->connection->error . ' sql:' . $sql . ' params:' . implode(" ",$array_params) . ' record:' . var_dump($record));
            };
            $statement->close(); 
            $result = $this->getLastInsertID();
        }
        return $result;
    }
        
    private function addRecordListData($tableName, $data) {
        $result = array();
        if (is_array($data)) {
            foreach ($data as $record) {
                $result[] = $this->addRecordItemData($tableName, $record);
            }
        }
        return $result;
    }     
        
    private function addChildRecordListData($tableName, $data, $parentColumn, $parentValue) {
        $result = array();
        foreach ($data as $record) {
            $result[] = $this->addRecordItemData($tableName, $record, $parentColumn, $parentValue);
        }
        return $result;
    }    
 
    private function newHeaderData($guid, $userID, $isPartner, $dataType, $belastingjaar, $geslacht, $naam) {
        $sql = "
            insert
              into aqopiapp.t_header
                 ( finsave_guid
                 , user_hash
                 , user_rid
                 , is_partner
                 , data_type
                 , mbd_belastingjaar
                 , mpo_geslacht
                 , mpo_naam
                 )
            select ?
                 , ?
                 , ?
                 , ?
                 , ?
                 , ?
                 , ?
                 , ?
        ";         
        $statement = $this->connection->prepare($sql);        
        $statement->bind_param(
          'ssiisiss'
        , $guid
        , $this->userHash
        , $userID
        , $isPartner
        , $dataType
        , $belastingjaar
        , $geslacht
        , $naam
        );        
        if (!$statement->execute()) {  
            throw new Exception('003 ' . $this->connection->error);
        }; 
        $statement->close(); 
        $result = $this->getLastInsertID();
        return $result;  
    }
   
    private function addChildRecordListResult($tableName, $data, $parentColumn, $parentValue) {
        $result = new stdClass();
        $result->data = new stdClass();
        try {                        
            $this->refreshTableColumnList($tableName);
            $result->data->aqopi = $this->addChildRecordListData($tableName, $data, $parentColumn, $parentValue);
            $result->data->finsave = Aqopi2Finsave::save($this->connection, $this->headerID, $tableName);
            $result->errorcode = '000';
            $result->message = 'No errors';
        } catch (Exception $e) {
            $result->data = '';
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    }
    
    private function addChildRecordItemResult($tableName, $data, $parentColumn, $parentValue) {
        $result = new stdClass();
        $result->data = new stdClass();
        try {                        
            $this->refreshTableColumnList($tableName);
            $result->data->aqopi = $this->addRecordItemData($tableName, $data, $parentColumn, $parentValue);
            $result->data->finsave = Aqopi2Finsave::save($this->connection, $this->headerID, $tableName);
            $result->errorcode = '000';
            $result->message = 'No errors';
        } catch (Exception $e) {
            $result->data = '';
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    }
            
    private function newMbdHeaderResult($guid, $userID, $isPartner, $belastingjaar) {
        $result = new stdClass();
        try {
            $result->data = $this->newHeaderData($guid, $userID, $isPartner, 'MBD', $belastingjaar, '', '');
            $result->errorcode = '000';
            $result->message = 'No errors';
        } catch (Exception $e) {
            $result->data = '';
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    }       
               
    private function newUwvHeaderResult($guid, $userID, $isPartner) {
        $result = new stdClass();
        try {
            $result->data = $this->newHeaderData($guid, $userID, $isPartner, 'UWV', 0, '', '');
            $result->errorcode = '000';
            $result->message = 'No errors';
        } catch (Exception $e) {
            $result->data = '';
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    }       
                   
    private function newMohHeaderResult($guid, $userID, $isPartner) {
        $result = new stdClass();
        try {
            $result->data = $this->newHeaderData($guid, $userID, $isPartner, 'MOH', 0, '', '');
            $result->errorcode = '000';
            $result->message = 'No errors';
        } catch (Exception $e) {
            $result->data = '';
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    }       
                      
    private function newMpoHeaderResult($guid, $userID, $isPartner, $geslacht, $naam) {
        $result = new stdClass();
        try {
            $result->data = $this->newHeaderData($guid, $userID, $isPartner, 'MPO', 0, $geslacht, $naam);
            $result->errorcode = '000';
            $result->message = 'No errors';
        } catch (Exception $e) {
            $result->data = '';
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    }  

    private function addRecordListResult($tableName, $data) {
        $result = new stdClass();
        $result->data = new stdClass();
        try {   
            $this->refreshTableColumnList($tableName);  
            $result->data->aqopi = $this->addRecordListData($tableName, $data);
            $result->data->finsave = Aqopi2Finsave::save($this->connection, $this->headerID, $tableName);
            $result->errorcode = '000';
        } catch (Exception $e) {
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    } 
    
    private function addRecordItemResult($tableName, $data) {
        $result = new stdClass();
        $result->data = new stdClass();
        try {                        
            $this->refreshTableColumnList($tableName);
            $result->data->aqopi = $this->addRecordItemData($tableName, $data);
            $result->data->finsave = Aqopi2Finsave::save($this->connection, $this->headerID, $tableName);
            $result->errorcode = '000';
        } catch (Exception $e) {
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    }      
    
    static public function addRecordList($connection, $userHash, $headerID, $tableName, $data) {
        $aqopiData = new AqopiData($connection, $userHash, $headerID);
        $result = $aqopiData->addRecordListResult($tableName, $data);
        return $result;         
    }
   
    static public function addRecordItem($connection, $userHash, $headerID, $tableName, $data) {
        $aqopiData = new AqopiData($connection, $userHash, $headerID);
        $result = $aqopiData->addRecordItemResult($tableName, $data);
        return $result;         
    }
               
    static public function newMbdHeader($connection, $userHash, $guid, $userID, $isPartner, $belastingjaar) {
        $aqopiData = new AqopiData($connection, $userHash, null);
        $result = $aqopiData->newMbdHeaderResult($guid, $userID, $isPartner, $belastingjaar);
        return $result;         
    }   
                   
    static public function newUwvHeader($connection, $userHash, $guid, $userID, $isPartner) {
        $aqopiData = new AqopiData($connection, $userHash, null);
        $result = $aqopiData->newUwvHeaderResult($guid, $userID, $isPartner);
        return $result;         
    }   
                   
    static public function newMohHeader($connection, $userHash, $guid, $userID, $isPartner) {
        $aqopiData = new AqopiData($connection, $userHash, null);
        $result = $aqopiData->newMohHeaderResult($guid, $userID, $isPartner);
        return $result;         
    }  
                       
    static public function newMpoHeader($connection, $userHash, $guid, $userID, $isPartner, $geslacht, $naam) {
        $aqopiData = new AqopiData($connection, $userHash, null);
        $result = $aqopiData->newMpoHeaderResult($guid, $userID, $isPartner, $geslacht, $naam);
        return $result;         
    }  
    
    static public function addChildRecordList($connection, $userHash, $headerID, $tableName, $data, $parentColumn, $parentValue)  {
        $aqopiData = new AqopiData($connection, $userHash, $headerID);
        $result = $aqopiData->addChildRecordListResult($tableName, $data, $parentColumn, $parentValue);
        return $result;         
    }
    
    static public function addChildRecordItem($connection, $userHash, $headerID, $tableName, $data, $parentColumn, $parentValue)  {
        $aqopiData = new AqopiData($connection, $userHash, $headerID);
        $result = $aqopiData->addChildRecordItemResult($tableName, $data, $parentColumn, $parentValue);
        return $result;         
    }    
}
