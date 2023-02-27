package com.flink.utils;

import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.OapiRobotSendRequest;
import com.dingtalk.api.response.OapiRobotSendResponse;
import com.taobao.api.ApiException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class DingTalkUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DingTalkUtil.class);
    /**
     * 钉钉消息采用TEXT格式
     */
    private static final String TEXT = "text";
    /**
     * 钉钉告警机器人webhook url
     */
    private static final String DEFAULT_WEBHOOK = "https://oapi.dingtalk.com/robot/send?access_token=e744fc77f890d6971b456b1b095a9a3d3c108a1547d0dd8b9c4203221c0a7b56";

    /**
     * 发送钉钉告警
     * @param alarmMsg 告警消息
     * @param dingWebhook 钉钉告警机器人地址
     * @return true/false
     */
    public static boolean sendAlarm(String alarmMsg, String dingWebhook) {
        return sendAlarm(alarmMsg, dingWebhook);
    }

    /**
     * 发送消息
     *
     * @param title
     * @param text
     * @param dingWebhook
     * @return
     */
    public static boolean sendAlarm(String title, String text, String dingWebhook) {
        if (StringUtils.isEmpty(dingWebhook)) {
            dingWebhook = DEFAULT_WEBHOOK;
        }
        DingTalkClient client = new DefaultDingTalkClient(dingWebhook);
        OapiRobotSendRequest request = new OapiRobotSendRequest();

        request.setMsgtype(TEXT);
        OapiRobotSendRequest.Text message = new OapiRobotSendRequest.Text();
        message.setContent(title+"\n"+text);
        request.setText(message);

        OapiRobotSendRequest.At at = new OapiRobotSendRequest.At();
        at.setIsAtAll(false);
        request.setAt(at);

        OapiRobotSendResponse response = null;
        try {
            response = client.execute(request);
        } catch (ApiException e) {
            LOG.error("发送钉钉告警失败" + e.getMessage());
        }
        if (Objects.isNull(response)) {
            return false;
        }
        return response.isSuccess();
    }
}
