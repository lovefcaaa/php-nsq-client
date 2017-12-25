<?php
/**
 * Nsq api
 * User: moyo
 * Date: 5/7/15
 * Time: 3:45 PM
 */

namespace Kdt\Iron\Queue\Adapter\Nsq;

use Kdt\Iron\Queue\Interfaces\AdapterInterface;
use Kdt\Iron\Queue\Message;

use nsqphp\Message\Message as NsqMessage;
use nsqphp\Exception\RequeueMessageException;

use Exception as SysException;

class Client implements AdapterInterface
{
    /**
     * @var Config
     */
    private $config = null;

    /**
     * Client constructor.
     */
    public function __construct()
    {
        $this->config = Config::getInstance();
    }

    /**
     * @param $topic
     * @param $message
     * @param $options
     * @return array
     */
    public function push($topic, $message, array $options = [])
    {//victor_fangc change 20171225
    	$a=function () use ($topic, $message) {
            $inst = InstanceMgr::getPubInstance($topic);
             $c = $inst->publish(
                $this->config->parseTopicName($topic),
                MsgFilter::getInstance()->getMsgObject($topic, $message)
            );
            return $c;
        };

        $b = HA::getInstance();
  
        $result = $b->pubRetrying($a, $topic, $options['max_retry'], $options['retry_delay_ms']);

        return $this->makePubResult($topic, $result);
    }

    /**
     * @param $topic
     * @param $messages
     * @param $options
     * @return array
     */
    public function bulk($topic, array $messages, array $options = [])
    {
        $result = HA::getInstance()->pubRetrying(function () use ($topic, $messages) {

            return InstanceMgr::getPubInstance($topic)->publish(
                $this->config->parseTopicName($topic),
                MsgFilter::getInstance()->getMsgObjectBag($topic, $messages)
            );

        }, $topic, $options['max_retry'], $options['retry_delay_ms']);

        return $this->makePubResult($topic, $result);
    }

    /**
     * @param $topic
     * @param callable $callback
     * @param $options
     * @return string
     */
    public function pop($topic, callable $callback, array $options = [])
    {//victor_fangc change 20171225
        // topic & channel
        if (is_array($topic))
        {
            list($topic, $channel) = $topic;
        }
        else
        {
            $channel = 'default';
        }
        $identify = $this->config->parseTopicName($topic).'-'.$channel;
        $msgCb = function (NsqMessage $m) use ($callback)
                {
                    $msg = (new Message(
                            $m->getId(),
                            $m->getTimestamp(),
                            $m->getAttempts(),
                            $m->getPayload()
                        ))
                        ->setTraceID(intval($m->getTraceId()))
                        ->setTag($m->getTag());
                    $callback($msg);
                };
                
        $a = function ($maxKeepSeconds) use ($topic, $channel, $msgCb, $options) {
            $lookupResult = Router::getInstance()->fetchSubscribeNodes($topic, $options['sub_partition']);
            $meta = current($lookupResult)['meta'];
            if (!$meta['extend_support']) {
                $options['tag'] = null;
            }
            $realTopic = $this->config->parseTopicName($topic);
            $d = InstanceMgr::getSubInstance($topic);
            
            $e = $d->subscribe(
                $lookupResult,
                $realTopic,
                $channel,
                $msgCb,
                $options['auto_delete'],
                $options['sub_ordered'],
                $options['msg_timeout'],
                $options['tag']
            );
            $e->run($maxKeepSeconds);
            return false;

        };        
        $b = HA::getInstance();
        $c = $b->subRetrying($a, $identify, $options['keep_seconds'], $options['max_retry'], $options['retry_delay']);
       
        return $c;
    }

    /**
     * exiting pop
     */
    public function stop()
    {
        InstanceMgr::getSubInstance()->stop();
    }

    /**
     * @param $messageId
     * @return bool
     */
    public function delete($messageId)
    {
        return InstanceMgr::getSubInstance()->deleteMessage($messageId);
    }

    /**
     * make delay
     * @param $seconds
     */
    public function later($seconds)
    {
        throw new RequeueMessageException($seconds * 1000);
    }

    /**
     * make retry
     */
    public function retry()
    {
        throw new RequeueMessageException(1);
    }

    /**
     * close all connections
     */
    public function close()
    {
        InstanceMgr::getSubInstance()->close();
    }

    /**
     * @param $topic
     * @return array
     */
    public function stats($topic)
    {
        $res = [];

        try {

            $nsqd = InstanceMgr::getPubInstance($topic);

            $hosts = Router::getInstance()->fetchPublishNodes($topic);

            foreach ($hosts as $host) {
                $res[$host['host']] = $nsqd->node_stats($host['host']);
            }

        } catch (SysException $e) {

        }

        return $res;
    }

    public function ping()
    {
        InstanceMgr::getSubInstance()->nop();
    }

    /**
     * @param $topic
     * @param $result
     * @return array
     */
    private function makePubResult($topic, array $result)
    {
        // check result
        $error_code = -1;
        $error_message = '';
        if ($result['success'])
        {
            $error_code = 0;
        }
        else if ($result['errors'])
        {
            $error_code = 1;
            $error_message = implode('|', $result['errors']);
            // logging
            InstanceMgr::getLoggerInstance()->error('[IRON] Actual failed via (PUB) : ['.$topic.'] ~ ' . $error_message);
        }
        // return result
        return [
            'error_code' => $error_code,
            'error_message' => $error_message
        ];
    }
}
