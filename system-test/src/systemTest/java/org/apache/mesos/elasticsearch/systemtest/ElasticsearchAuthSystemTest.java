package org.apache.mesos.elasticsearch.systemtest;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.collections4.ListUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ElasticsearchAuthSystemTest {
    protected static final int NODE_COUNT = 3;

    public static abstract class ACLStringPredicate {
        public abstract Object toJSONAST();
    }

    public static final class ACLStringPredicateAny extends ACLStringPredicate {
        public ACLStringPredicateAny() {}
        public Object toJSONAST() {
            return new TreeMap<String,String>() {{ put("type", "ANY"); }};
        }
    }

    public static final class ACLStringPredicateNone extends ACLStringPredicate {
        public ACLStringPredicateNone() {}
        public Object toJSONAST() {
            return new TreeMap<String,String>() {{ put("type", "NONE"); }};
        }
    }

    public static final class ACLStringPredicateValues extends ACLStringPredicate {
        private List<String> values;
        public ACLStringPredicateValues(List<String> values) {
            this.values = values;
        }
        public Object toJSONAST() {
            return new TreeMap<String,Object>() {{ put("values", values); }};
        }
    }

    public static class ACLRegisterFrameworkPredicate {
        private ACLStringPredicate principals;
        private ACLStringPredicate roles;
        public ACLRegisterFrameworkPredicate(ACLStringPredicate principals, ACLStringPredicate roles) {
            this.principals = principals;
            this.roles = roles;
        }
        public Object toJSONAST() {
            return new TreeMap<String,Object>() {{
                put("principals", principals.toJSONAST());
                put("roles", roles.toJSONAST());
            }};
        }
    }

    public static class ACLRunTaskPredicate {
        private ACLStringPredicate principals;
        private ACLStringPredicate users;
        public ACLRunTaskPredicate(ACLStringPredicate principals, ACLStringPredicate users) {
            this.principals = principals;
            this.users = users;
        }
        public Object toJSONAST() {
            return new TreeMap<String,Object>() {{
                put("principals", principals.toJSONAST());
                put("users", users.toJSONAST());
            }};
        }
    }

    public static class ACLShutdownFrameworkPredicate {
        private ACLStringPredicate principals;
        private ACLStringPredicate framework_principals;
        public ACLShutdownFrameworkPredicate(ACLStringPredicate principals, ACLStringPredicate framework_principals) {
            this.principals = principals;
            this.framework_principals = framework_principals;
        }
        public Object toJSONAST() {
            return new TreeMap<String,Object>() {{
                put("principals", principals.toJSONAST());
                put("framework_principals", framework_principals.toJSONAST());
            }};
        }
    }

    public static class ACL {
        private boolean permissive;
        private List<ACLRegisterFrameworkPredicate> register_frameworks;
        private List<ACLRunTaskPredicate> run_tasks;
        private List<ACLShutdownFrameworkPredicate> shutdown_frameworks;
        public ACL(boolean permissive, List<ACLRegisterFrameworkPredicate> register_frameworks, List<ACLRunTaskPredicate> run_tasks, List<ACLShutdownFrameworkPredicate> shutdown_frameworks) {
            this.permissive = permissive;
            this.register_frameworks = register_frameworks;
            this.run_tasks = run_tasks;
            this.shutdown_frameworks = shutdown_frameworks;
        }
        public Object toJSONAST() {
            return new TreeMap<String,Object>() {{
                put("permissive", permissive);
                put("register_frameworks", register_frameworks.stream().map(ACLRegisterFrameworkPredicate::toJSONAST).collect(Collectors.toList()));
                put("run_tasks", run_tasks.stream().map(ACLRunTaskPredicate::toJSONAST).collect(Collectors.toList()));
                put("shutdown_frameworks", shutdown_frameworks.stream().map(ACLShutdownFrameworkPredicate::toJSONAST).collect(Collectors.toList()));
            }};
        }
    }

    protected static final MesosClusterConfig CONFIG = MesosClusterConfig.builder()
            .numberOfSlaves(NODE_COUNT)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .extraEnvironmentVariables(new TreeMap<String, String>() {
                {
                    this.put("MESOS_ROLES", "*,testRole");

                    // This policy allows frameworks to register with principal "testPrincipal" and role "testRole",
                    // disallows all other framework registrations,
                    // and allows all other kinds of action.
                    ACL acl = new ACL(
                            false,
                            Arrays.asList(new ACLRegisterFrameworkPredicate(new ACLStringPredicateValues(Arrays.asList("testPrincipal")), new ACLStringPredicateValues(Arrays.asList("testRole")))),
                            Arrays.asList(new ACLRunTaskPredicate(new ACLStringPredicateAny(), new ACLStringPredicateAny())),
                            Arrays.asList(new ACLShutdownFrameworkPredicate(new ACLStringPredicateAny(), new ACLStringPredicateAny()))
                    );

                    String aclJSONString;

                    try {
                        aclJSONString = (new ObjectMapper()).writeValueAsString(acl.toJSONAST());
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }

                    this.put("MESOS_ACLS", aclJSONString);
                }
            }).build();

    public static final Logger LOGGER = Logger.getLogger(ElasticsearchAuthSystemTest.class);

    @Rule
    public final MesosCluster CLUSTER = new MesosCluster(CONFIG);

    @Before
    public void before() throws Exception {
        CLUSTER.injectImage("mesos/elasticsearch-executor");
    }

    @After
    public void after() {
        CLUSTER.stop();
    }

    @Test
    private void testFrameworkCanRegisterWithCorrectPrincipalAndRole() throws UnirestException, JsonParseException, JsonMappingException {
        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(
                CLUSTER.getConfig().dockerClient,
                CLUSTER.getMesosContainer().getIpAddress(),
                "testRole",
                "testPrincipal"
        );
        CLUSTER.addAndStartContainer(scheduler);
        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":31100");

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> CLUSTER.getStateInfo().getFramework("elasticsearch") != null);
        Assert.assertEquals("testRole", CLUSTER.getStateInfo().getFramework("elasticsearch").getRole());
        //Assert.assertEquals("testPrincipal", CLUSTER.getStateInfo().getFramework("elasticsearch").getPrincipal());
        scheduler.remove();
    }
}
