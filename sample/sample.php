<?php

require __DIR__.'/../vendor/autoload.php';


$config = [
    'lookupd_pool' => [
        'global'
    ],

    'rw_strategy' => [
        'business_test' => ['global' => 'rw'],
    ],

    'topic' => [
        'business_test' => 'business_test',
    ]
];

$setting = [
    'nsq.server.lookupd.global' => 'http://XXXXXXXXXXXXXXXXXXXuookup:4161',
    'nsq.monitor.msg-bag' => [ 'nums' => 1000, 'size' => 65536 ]
];


use Kdt\Iron\Queue\Queue;
use Kdt\Iron\Queue\Message;
use Kdt\Iron\Queue\Adapter\Nsq\Config;

$topic = 'business_test';
$channel = 'business_test_chan';
$message0 = 'hello world';
$message1 = ['hello', 'world'];
Config::getInstance()->setGlobalSetting($setting);
Config::getInstance()->addTopicConfig($topic, $config);
var_dump(Queue::push($topic, $message0));
var_dump(Queue::push($topic, $message1));


$result = Queue::pop([$topic, $channel], function($message) {
    //var_dump($message);
    
    printf("[%s](%s): %s\n", $message->getId(), $message->getTag(), $message->getPayload());
    
    var_dump($message->getId());
    
    Queue::delete($message->getId());
    printf("ack %s\n", $message->getId());
    
    Queue::exitPop();
},
[
  "auto_delete"=>false,
  "keep_seconds"=>10,
  "max_retry"=>2,
  "retry_delay"=>5,
  "sub_ordered"=>true,
  "sub_partition"=>NULL,
  "msg_timeout"=>NULL,
  "tag"=>NULL
]
);

var_dump($result);

