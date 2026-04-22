import React, {Component} from 'react'
import {Input, Modal, Switch, Form, Button, Tabs, Select, Checkbox, Divider, message} from 'antd'
import axios from "axios";
import {BACKEND_ORIGIN} from '../../config';

export default class Settings extends Component {
    flinkFormRef = React.createRef();
    inputStreamsFormRef = React.createRef();

    state = {
        visible: false
    };

    showModal = () => {
        this.setState({
            visible: true
        });

        axios.get(`${BACKEND_ORIGIN}/r/settings`).then(
            res => {
                if (this.flinkFormRef.current) {
                    this.flinkFormRef.current.setFieldsValue(res.data);
                }
                if (this.inputStreamsFormRef.current) {
                    this.inputStreamsFormRef.current.setFieldsValue(res.data);
                }
            }
        ).catch(err => {
            const detail = err.response?.data?.error || err.response?.data || err.message;
            message.error(String(detail));
        });
    };

    hideModal = () => {
        this.setState({
            visible: false,
        });
    };

    handleSaveSettings = () => {
        // TODO
        this.setState({
            visible: false,
        });
    };

    handleRemoteFlinkChecked = (checked, ev) => {

    }

    onFinish = (values) => {
        axios.post(`${BACKEND_ORIGIN}/r/save_settings`, (values), {
            headers: {
                'Access-Control-Allow-Origin': '*',
            }
        }).then(
            res => {
                if (this.flinkFormRef.current) {
                    this.flinkFormRef.current.setFieldsValue(res.data);
                }
                if (this.inputStreamsFormRef.current) {
                    this.inputStreamsFormRef.current.setFieldsValue(res.data);
                }
                message.success("Settings saved");
                this.hideModal();
            }
        ).catch(err => {
            const detail = err.response?.data?.error || err.response?.data || err.message;
            message.error(String(detail));
        })
    }

    onFinishFailed = (errorInfo) => {
        console.log('Failed:', errorInfo);
    }

    tabChange = (key) => {

    }

    render() {
        const layout = {
            labelCol: {span: 8},
            wrapperCol: {span: 16},
        };

        const {TabPane} = Tabs;
        const {Option} = Select;
        const plainOptions = ['File', 'Socket', 'Kafka'];

        function onChange(checkedValues) {
            console.log('checked = ', checkedValues);
        }

        return (
            <div>
                <div onClick={this.showModal.bind(this)}>Settings</div>
                <Modal
                    title="Settings"
                    visible={this.state.visible}
                    onOk={this.handleSaveSettings}
                    onCancel={this.hideModal}
                    okText="Save"
                    footer={null}
                    width={700}
                >
                    <Tabs defaultActiveKey="1" type="card" onChange={this.tabChange}>

                        <TabPane tab="Flink Cluster" key="1">
                            <Form
                                ref={this.flinkFormRef}
                                {...layout}
                                name="basic"
                                initialValues={{remember: true}}
                                onFinish={this.onFinish}
                                onFinishFailed={this.onFinishFailed}
                            >
                                <Form.Item
                                    label="Remote Flink"
                                    name="remote_flink"
                                    valuePropName="checked"
                                >
                                    <Switch/>
                                </Form.Item>

                                <Form.Item
                                    label="Remote Flink Url"
                                    name="remote_flink_url"
                                >
                                    <Input placeholder="47.93.121.10:8081" />
                                </Form.Item>

                                <Form.Item
                                    label="Flink Home Path"
                                    name="flink_home_path"
                                >
                                    <Input placeholder="/path/to/flink-1.11.2"/>
                                </Form.Item>

                                <Form.Item
                                    label="Flink Parallelism"
                                    name="flink_parallelism"
                                >
                                    <Input placeholder="1"/>
                                </Form.Item>
                                <Button type="primary" htmlType="submit">Save</Button> <span> </span>
                                <Button type="primary" onClick={this.hideModal}>Cancel</Button>

                            </Form>
                        </TabPane>

                        <TabPane tab="Input Streams Config" key="2">
                            <Form
                                ref={this.inputStreamsFormRef}
                                {...layout}
                                name="basic"
                                initialValues={{remember: true}}
                                onFinish={this.onFinish}
                                onFinishFailed={this.onFinishFailed}
                            >
                                <Form.Item
                                    label="Scale Factor"
                                    name="scale_factor"
                                    initialValue = "0.01"
                                >
                                    <Input placeholder="0.01"/>
                                </Form.Item>

                                <Form.Item
                                    label="Streams Types"
                                    name="streams_types"
                                    initialValue = "insert_only"
                                >
                                    <Select defaultValue="insert_only">
                                        <Option value="sliding_windows">sliding windows</Option>
                                        <Option value="insert_only">insert only</Option>
                                    </Select>
                                </Form.Item>

                                <Button type="primary" htmlType="submit">Save</Button> <span> </span>
                                <Button type="primary" onClick={this.hideModal}>Cancel</Button>

                            </Form>
                        </TabPane>

                        <TabPane tab="Data Source" key="3">
                            <Form
                                {...layout}
                                name="basic"
                                initialValues={{remember: true}}
                                onFinish={this.onFinish}
                                onFinishFailed={this.onFinishFailed}
                            >
                                <Form.Item
                                    label="Input Methods"
                                    name="input_methods"
                                >
                                    <Checkbox defaultChecked>File</Checkbox>
                                    <Checkbox >Socket</Checkbox>
                                    <Checkbox >Kafka</Checkbox>
                                </Form.Item>

                                <Form.Item
                                    label="Input Data File"
                                    name="input_data_file"
                                    initialValue = "/Users/chaoqi/data/input_data.csv"
                                >
                                    <Input placeholder="/Users/chaoqi/data/input_data.csv"/>
                                </Form.Item>

                                <Form.Item
                                    label="Input Socket Port"
                                    name="input_socket_port"
                                    initialValue ="5002"
                                >
                                    <Input placeholder="5002"/>
                                </Form.Item>

                                <Form.Item
                                    label="Input Kafka Zookeeper "
                                    name="input_kafka_zookeeper"
                                    initialValue ="localhost:2181"
                                >
                                    <Input placeholder="localhost:2181"/>
                                </Form.Item>

                                <Form.Item
                                    label="Input Kafka Topic "
                                    name="input_kafka_topic"
                                    initialValue ="cquirrel_kafka_data_input"
                                >
                                    <Input placeholder="cquirrel_kafka_data_input"/>
                                </Form.Item>

                                <Button type="primary" htmlType="submit">Save</Button> <span> </span>
                                <Button type="primary" onClick={this.hideModal}>Cancel</Button>
                            </Form>
                        </TabPane>

                        <TabPane tab="Data Sink" key="4">
                            <Form
                                {...layout}
                                name="basic"
                                initialValues={{remember: true}}
                                onFinish={this.onFinish}
                                onFinishFailed={this.onFinishFailed}
                            >
                                <Form.Item
                                    label="Output Methods"
                                    name="output_methods"
                                >
                                    <Checkbox defaultChecked>File</Checkbox>
                                    <Checkbox >Socket</Checkbox>
                                    <Checkbox >Kafka</Checkbox>
                                </Form.Item>

                                <Form.Item
                                    label="Output Data File"
                                    name="output_data_file"
                                    initialValue = "/Users/chaoqi/data/output_data.csv"
                                >
                                    <Input placeholder="/Users/chaoqi/data/output_data.csv"/>
                                </Form.Item>

                                <Form.Item
                                    label="Output Socket Port"
                                    name="output_socket_port"
                                    initialValue ="5001"
                                >
                                    <Input placeholder="5001"/>
                                </Form.Item>

                                <Form.Item
                                    label="Output Kafka Zookeeper "
                                    name="output_kafka_zookeeper"
                                    initialValue ="localhost:2181"
                                >
                                    <Input placeholder="localhost:2181"/>
                                </Form.Item>

                                <Form.Item
                                    label="Output Kafka Topic "
                                    name="output_kafka_topic"
                                    initialValue ="cquirrel_kafka_data_output"
                                >
                                    <Input placeholder="cquirrel_kafka_data_output"/>
                                </Form.Item>

                                <Button type="primary" htmlType="submit">Save</Button> <span> </span>
                                <Button type="primary" onClick={this.hideModal}>Cancel</Button>
                            </Form>
                        </TabPane>


                    </Tabs>



                </Modal>
            </div>

        )
    }
}
